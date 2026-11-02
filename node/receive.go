package node

import (
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	pb "github.com/rudderlabs/keydb/proto"
)

type receiveStream = grpc.ClientStreamingServer[pb.SnapshotChunk, pb.ReceiveSnapshotResponse]

// receive loads the chunks into the cache as they arrive, so the node never holds a whole range in memory, then
// stores the range's marker. The caller keeps Get and Put off the cache meanwhile: with the write lock, or on a
// shadow with loadMu and the read lock, so that GetNodeInfo still answers.
func (s *Service) receive(stream receiveStream, first *pb.SnapshotChunk) error {
	s.receiving.Store(true)
	defer s.receiving.Store(false)
	var (
		hashRange   = first.HashRange
		timestamp   = first.Timestamp
		replication = first.SourceAddress != "" // replication streams name their source, and only fill a shadow
		pw          *io.PipeWriter
		loadDone    chan error
	)
	write := func(data []byte) error {
		if len(data) == 0 {
			return nil
		}
		if pw == nil { // start loading on the first chunk with data, an empty range loads nothing
			var pr *io.PipeReader
			pr, pw = io.Pipe()
			loadDone = make(chan error, 1)
			go func() {
				err := s.cache.LoadSnapshot(stream.Context(), pr)
				// unblock the writer if the load stops early
				_ = pr.CloseWithError(errors.Join(err, io.ErrClosedPipe))
				loadDone <- err
			}()
		}
		_, err := pw.Write(data)
		return err
	}
	fail := func(msg string, err error) error {
		if pw != nil {
			_ = pw.CloseWithError(err)
			<-loadDone
		}
		s.logger.Errorn(msg, logger.NewIntField("hashRange", hashRange), obskit.Error(err))
		return stream.SendAndClose(&pb.ReceiveSnapshotResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("%s: %v", msg, err),
		})
	}

	if err := write(first.Data); err != nil {
		return fail("loading snapshot", err)
	}
	for chunk := first; !chunk.IsLast; {
		var err error
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			// without the last chunk the range may be partial, it must not be marked as synced
			return fail("receiving chunk", errors.New("the stream ended before the last chunk"))
		}
		if err != nil {
			return fail("receiving chunk", err)
		}
		timestamp = chunk.Timestamp
		if replication && !s.isShadow() {
			// the node was promoted mid-receive: stop loading so it can serve, the sender retries the range
			return fail("receiving chunk", errors.New("the shadow became active during the receive"))
		}
		if err := write(chunk.Data); err != nil {
			return fail("loading snapshot", err)
		}
	}

	if pw != nil {
		_ = pw.Close()
		if err := <-loadDone; err != nil {
			pw = nil // the load already ended
			return fail("loading snapshot", err)
		}
	}

	if err := s.advanceSnapshotTimestamp(hashRange, timestamp); err != nil {
		s.logger.Warnn("Setting snapshot timestamp", logger.NewIntField("hashRange", hashRange), obskit.Error(err))
		// Continue anyway, the data was loaded successfully
	}
	logn := s.logger.Infon
	if replication {
		logn = s.logger.Debugn // replication sends every range of every pass, the active node logs each pass
	}
	logn("Successfully received and loaded snapshot",
		logger.NewIntField("hashRange", hashRange),
		logger.NewIntField("timestamp", int64(timestamp)),
	)

	return stream.SendAndClose(&pb.ReceiveSnapshotResponse{Success: true})
}

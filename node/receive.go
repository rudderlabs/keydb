package node

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	pb "github.com/rudderlabs/keydb/proto"
)

type receiveStream = grpc.ClientStreamingServer[pb.SnapshotChunk, pb.ReceiveSnapshotResponse]

// receiveBuffered buffers the whole range before loading it. The caller holds the write lock.
func (s *Service) receiveBuffered(stream receiveStream, first *pb.SnapshotChunk) error {
	var (
		hashRange = first.HashRange
		timestamp = first.Timestamp
		buf       bytes.Buffer
	)
	buf.Write(first.Data)

	for chunk := first; !chunk.IsLast; {
		var err error
		chunk, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.logger.Errorn("Receiving snapshot chunk", obskit.Error(err))
			return stream.SendAndClose(&pb.ReceiveSnapshotResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("receiving chunk: %v", err),
			})
		}
		timestamp = chunk.Timestamp
		buf.Write(chunk.Data)
	}

	if buf.Len() > 0 {
		s.logger.Infon("Loading received snapshot",
			logger.NewIntField("hashRange", hashRange),
			logger.NewIntField("bufferSize", int64(buf.Len())),
		)
		// LoadSnapshot handles decompression internally if compression is enabled
		if err := s.cache.LoadSnapshot(stream.Context(), &buf); err != nil {
			s.logger.Errorn("Loading snapshot", obskit.Error(err))
			return stream.SendAndClose(&pb.ReceiveSnapshotResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("loading snapshot: %v", err),
			})
		}
	}

	return s.finishReceive(stream, hashRange, timestamp, s.logger.Infon)
}

// receiveStreaming loads the chunks into the cache as they arrive, so a shadow never holds a whole range in memory.
// The caller holds the read lock, so that GetNodeInfo still answers, and loadMu, which keeps Get and Put off the
// cache while it loads.
func (s *Service) receiveStreaming(stream receiveStream, first *pb.SnapshotChunk) error {
	s.receiving.Store(true)
	defer s.receiving.Store(false)
	var (
		hashRange = first.HashRange
		timestamp = first.Timestamp
		pw        *io.PipeWriter
		loadDone  chan error
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
		if s.role() != pb.NodeRole_SHADOW {
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
			s.logger.Errorn("Loading snapshot", logger.NewIntField("hashRange", hashRange), obskit.Error(err))
			return stream.SendAndClose(&pb.ReceiveSnapshotResponse{
				Success:      false,
				ErrorMessage: fmt.Sprintf("loading snapshot: %v", err),
			})
		}
	}

	// replication sends every range of every pass for as long as a move lasts, the active node logs each pass
	return s.finishReceive(stream, hashRange, timestamp, s.logger.Debugn)
}

// finishReceive stores the marker for future incremental snapshots and closes the stream, logging with logn.
func (s *Service) finishReceive(
	stream receiveStream, hashRange int64, timestamp uint64, logn func(string, ...logger.Field),
) error {
	if err := s.advanceSnapshotTimestamp(hashRange, timestamp); err != nil {
		s.logger.Warnn("Setting snapshot timestamp", logger.NewIntField("hashRange", hashRange), obskit.Error(err))
		// Continue anyway, the data was loaded successfully
	}

	logn("Successfully received and loaded snapshot",
		logger.NewIntField("hashRange", hashRange),
		logger.NewIntField("timestamp", int64(timestamp)),
	)

	return stream.SendAndClose(&pb.ReceiveSnapshotResponse{Success: true})
}

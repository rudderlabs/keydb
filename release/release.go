package release

var (
	version                    = "Not an official release. Get the latest release from the github repo."
	commit, buildDate, builtBy string
)

type Info struct {
	Version   string
	Commit    string
	BuildDate string
	BuiltBy   string
}

func NewInfo() *Info {
	return &Info{
		Version:   version,
		Commit:    commit,
		BuildDate: buildDate,
		BuiltBy:   builtBy,
	}
}

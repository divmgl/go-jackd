package jackd

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn    net.Conn
	buffer  *bufio.ReadWriter
	scanner *bufio.Scanner
	mutex   *sync.Mutex
}

type PutOpts struct {
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}

func DefaultPutOpts() PutOpts {
	return PutOpts{
		Priority: 0,
		Delay:    0,
		TTR:      60 * time.Second,
	}
}

type ReleaseOpts struct {
	Priority uint32
	Delay    time.Duration
}

func DefaultReleaseOpts() ReleaseOpts {
	return ReleaseOpts{
		Priority: 0,
		Delay:    0,
	}
}

type JobStats struct {
	ID       uint32 `yaml:"id"`
	Tube     string `yaml:"tube"`
	State    string `yaml:"state"`
	Priority uint32 `yaml:"pri"`
	Age      uint32 `yaml:"age"`
	Delay    uint32 `yaml:"delay"`
	TTR      uint32 `yaml:"ttr"`
	TimeLeft uint32 `yaml:"time-left"`
	File     uint32 `yaml:"file"`
	Reserves uint32 `yaml:"reserves"`
	Timeouts uint32 `yaml:"timeouts"`
	Releases uint32 `yaml:"releases"`
	Buries   uint32 `yaml:"buries"`
	Kicks    uint32 `yaml:"kicks"`
}

type TubeStats struct {
	Name                string `yaml:"name"`
	CurrentJobsUrgent   uint32 `yaml:"current-jobs-urgent"`
	CurrentJobsReady    uint32 `yaml:"current-jobs-ready"`
	CurrentJobsReserved uint32 `yaml:"current-jobs-reserved"`
	CurrentJobsDelayed  uint32 `yaml:"current-jobs-delayed"`
	CurrentJobsBuried   uint32 `yaml:"current-jobs-buried"`
	TotalJobs           uint32 `yaml:"total-jobs"`
	CurrentUsing        uint32 `yaml:"current-using"`
	CurrentWaiting      uint32 `yaml:"current-waiting"`
	CurrentWatching     uint32 `yaml:"current-watching"`
	Pause               uint32 `yaml:"pause"`
	CmdDelete           uint32 `yaml:"cmd-delete"`
	CmdPauseTube        uint32 `yaml:"cmd-pause-tube"`
	PauseTimeLeft       uint32 `yaml:"pause-time-left"`
}

type SystemStats struct {
	// Current job counts
	CurrentJobsUrgent   uint32 `yaml:"current-jobs-urgent"`
	CurrentJobsReady    uint32 `yaml:"current-jobs-ready"`
	CurrentJobsReserved uint32 `yaml:"current-jobs-reserved"`
	CurrentJobsDelayed  uint32 `yaml:"current-jobs-delayed"`
	CurrentJobsBuried   uint32 `yaml:"current-jobs-buried"`

	// Command statistics
	CmdPut                uint32 `yaml:"cmd-put"`
	CmdPeek               uint32 `yaml:"cmd-peek"`
	CmdPeekReady          uint32 `yaml:"cmd-peek-ready"`
	CmdPeekDelayed        uint32 `yaml:"cmd-peek-delayed"`
	CmdPeekBuried         uint32 `yaml:"cmd-peek-buried"`
	CmdReserve            uint32 `yaml:"cmd-reserve"`
	CmdReserveWithTimeout uint32 `yaml:"cmd-reserve-with-timeout"`
	CmdTouch              uint32 `yaml:"cmd-touch"`
	CmdUse                uint32 `yaml:"cmd-use"`
	CmdWatch              uint32 `yaml:"cmd-watch"`
	CmdIgnore             uint32 `yaml:"cmd-ignore"`
	CmdDelete             uint32 `yaml:"cmd-delete"`
	CmdRelease            uint32 `yaml:"cmd-release"`
	CmdBury               uint32 `yaml:"cmd-bury"`
	CmdKick               uint32 `yaml:"cmd-kick"`
	CmdStats              uint32 `yaml:"cmd-stats"`
	CmdStatsJob           uint32 `yaml:"cmd-stats-job"`
	CmdStatsTube          uint32 `yaml:"cmd-stats-tube"`
	CmdListTubes          uint32 `yaml:"cmd-list-tubes"`
	CmdListTubeUsed       uint32 `yaml:"cmd-list-tube-used"`
	CmdListTubesWatched   uint32 `yaml:"cmd-list-tubes-watched"`
	CmdPauseTube          uint32 `yaml:"cmd-pause-tube"`

	// Job statistics
	JobTimeouts uint32 `yaml:"job-timeouts"`
	TotalJobs   uint32 `yaml:"total-jobs"`
	MaxJobSize  uint32 `yaml:"max-job-size"`

	// Current state
	CurrentTubes       uint32 `yaml:"current-tubes"`
	CurrentConnections uint32 `yaml:"current-connections"`
	CurrentProducers   uint32 `yaml:"current-producers"`
	CurrentWorkers     uint32 `yaml:"current-workers"`
	CurrentWaiting     uint32 `yaml:"current-waiting"`
	TotalConnections   uint32 `yaml:"total-connections"`

	// Process information
	PID         uint32  `yaml:"pid"`
	Version     string  `yaml:"version"`
	RUsageUtime float64 `yaml:"rusage-utime"`
	RUsageStime float64 `yaml:"rusage-stime"`
	Uptime      uint32  `yaml:"uptime"`

	// Binlog information
	BinlogOldestIndex     uint32 `yaml:"binlog-oldest-index"`
	BinlogCurrentIndex    uint32 `yaml:"binlog-current-index"`
	BinlogMaxSize         uint32 `yaml:"binlog-max-size"`
	BinlogRecordsWritten  uint32 `yaml:"binlog-records-written"`
	BinlogRecordsMigrated uint32 `yaml:"binlog-records-migrated"`

	// System information
	Draining bool   `yaml:"draining"`
	ID       string `yaml:"id"`
	Hostname string `yaml:"hostname"`
	OS       string `yaml:"os"`
	Platform string `yaml:"platform"`
}

// TubeList represents the YAML response from list-tubes command
// which returns a list of all existing tube names
type TubeList []string

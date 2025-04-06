package jackd

import (
	"errors"
	"strings"
)

var NoErrs = make([]string, 0)
var ErrTubeNameTooBig = errors.New("tube name over 200 bytes")

func validateTubeName(tube string) error {
	tubeNameBytes := []byte(tube)
	if len(tubeNameBytes) > MaxTubeName {
		return ErrTubeNameTooBig
	}
	return nil
}

func validate(resp string, additionalErrorStrings []string) error {
	errorStrings := append(
		/* Generic command errors */
		[]string{OutOfMemory, InternalError, BadFormat, UnknownCommand},
		additionalErrorStrings...,
	)

	for _, errorString := range errorStrings {
		if strings.HasPrefix(resp, errorString) {
			if err, ok := errorMap[errorString]; ok {
				return err
			}

			return errors.New(errorString)
		}
	}

	return nil
}

//goland:noinspection ALL
var (
	/* Generic command errors */
	OutOfMemory       = "OUT_OF_MEMORY"
	ErrOutOfMemory    = errors.New("out of memory")
	InternalError     = "INTERNAL_ERROR"
	ErrInternalError  = errors.New("internal error")
	BadFormat         = "BAD_FORMAT"
	ErrBadFormat      = errors.New("bad format")
	UnknownCommand    = "UNKNOWN_COMMAND"
	ErrUnknownCommand = errors.New("unknown command")

	/* Reservation errors */
	DeadlineSoon    = "DEADLINE_SOON"
	ErrDeadlineSoon = errors.New("deadline soon")
	TimedOut        = "TIMED_OUT"
	ErrTimedOut     = errors.New("timed out")

	/* Put errors */
	Buried          = "BURIED"
	ErrBuried       = errors.New("job buried")
	ExpectedCRLF    = "EXPECTED_CRLF"
	ErrExpectedCRLF = errors.New("put command expects CRLF")
	JobTooBig       = "JOB_TOO_BIG"
	ErrJobTooBig    = errors.New("job is too big")
	Draining        = "DRAINING"
	ErrDraining     = errors.New("tube is draining")

	/* Delete errors */
	NotFound    = "NOT_FOUND"
	ErrNotFound = errors.New("job not found")

	/* Ignore errors */
	NotIgnored    = "NOT_IGNORED"
	ErrNotIgnored = errors.New("tube not ignored")
)

var errorMap = map[string]error{
	OutOfMemory:    ErrOutOfMemory,
	InternalError:  ErrInternalError,
	BadFormat:      ErrBadFormat,
	UnknownCommand: ErrUnknownCommand,

	DeadlineSoon: ErrDeadlineSoon,
	TimedOut:     ErrTimedOut,

	Buried:       ErrBuried,
	ExpectedCRLF: ErrExpectedCRLF,
	JobTooBig:    ErrJobTooBig,
	Draining:     ErrDraining,

	NotFound: ErrNotFound,

	NotIgnored: ErrNotIgnored,
}

package jackd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

func Dial(ctx context.Context, addr string) (*Client, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	scanner := bufio.NewScanner(reader)
	scanner.Split(splitCRLF)

	return &Client{
		conn: conn,
		buffer: bufio.NewReadWriter(
			reader,
			bufio.NewWriter(conn),
		),
		scanner: scanner,
		mutex:   new(sync.Mutex),
	}, nil
}

func (jackd *Client) Put(ctx context.Context, body []byte, opts PutOpts) (uint32, error) {
	command := []byte(fmt.Sprintf(
		"put %d %d %d %d\r\n",
		opts.Priority,
		uint(opts.Delay.Seconds()),
		uint(opts.TTR.Seconds()),
		len(body)),
	)
	return jackd.executeCommandWithPutResponse(ctx, command, body, []string{
		Buried,
		ExpectedCRLF,
		JobTooBig,
		Draining,
	})
}

func (jackd *Client) Release(ctx context.Context, job uint32, opts ReleaseOpts) error {
	command := []byte(fmt.Sprintf(
		"release %d %d %d\r\n",
		job,
		opts.Priority,
		uint32(opts.Delay.Seconds()),
	))
	return jackd.executeCommand(ctx, command, "RELEASED", []string{Buried, NotFound})
}

func (jackd *Client) Delete(ctx context.Context, job uint32) error {
	command := []byte(fmt.Sprintf("delete %d\r\n", job))
	return jackd.executeCommand(ctx, command, "DELETED", []string{NotFound})
}

func (jackd *Client) Touch(ctx context.Context, job uint32) error {
	command := []byte(fmt.Sprintf("touch %d\r\n", job))
	return jackd.executeCommand(ctx, command, "TOUCHED", []string{NotFound})
}

func (jackd *Client) Kick(ctx context.Context, numJobs uint32) (kicked uint32, err error) {
	command := []byte(fmt.Sprintf("kick %d\r\n", numJobs))
	return jackd.executeCommandWithUint32Response(ctx, command, "KICKED %d", NoErrs)
}

func (jackd *Client) KickJob(ctx context.Context, id uint32) error {
	command := []byte(fmt.Sprintf("kick-job %d\r\n", id))
	return jackd.executeCommand(ctx, command, "KICKED", []string{NotFound})
}

func (jackd *Client) Bury(ctx context.Context, job uint32, priority uint32) error {
	command := []byte(fmt.Sprintf("bury %d %d\r\n", job, priority))
	return jackd.executeCommand(ctx, command, "BURIED", []string{NotFound})
}

func (jackd *Client) Watch(ctx context.Context, tube string) (watched uint32, err error) {
	if err = validateTubeName(tube); err != nil {
		return
	}
	command := []byte(fmt.Sprintf("watch %s\r\n", tube))
	return jackd.executeCommandWithUint32Response(ctx, command, "WATCHING %d", NoErrs)
}

func (jackd *Client) Use(ctx context.Context, tube string) (usingTube string, err error) {
	if err = validateTubeName(tube); err != nil {
		return
	}
	command := []byte(fmt.Sprintf("use %s\r\n", tube))
	return jackd.executeCommandWithStringResponse(ctx, command, "USING %s", NoErrs)
}

func (jackd *Client) PauseTube(ctx context.Context, tube string, delay time.Duration) error {
	command := []byte(fmt.Sprintf(
		"pause-tube %s %d\r\n",
		tube,
		uint32(delay.Seconds()),
	))
	return jackd.executeCommand(ctx, command, "PAUSED", []string{NotFound})
}

func (jackd *Client) Ignore(ctx context.Context, tube string) (watched uint32, err error) {
	if err = validateTubeName(tube); err != nil {
		return
	}
	command := []byte(fmt.Sprintf("ignore %s\r\n", tube))
	return jackd.executeCommandWithUint32Response(ctx, command, "WATCHING %d", []string{NotIgnored})
}

func (jackd *Client) Peek(ctx context.Context, job uint32) (uint32, []byte, error) {
	command := []byte(fmt.Sprintf("peek %d\r\n", job))
	return jackd.responseJobChunkWithContext(ctx, command, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekReady(ctx context.Context) (uint32, []byte, error) {
	command := []byte("peek-ready\r\n")
	return jackd.responseJobChunkWithContext(ctx, command, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekDelayed(ctx context.Context) (uint32, []byte, error) {
	command := []byte("peek-delayed\r\n")
	return jackd.responseJobChunkWithContext(ctx, command, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekBuried(ctx context.Context) (uint32, []byte, error) {
	command := []byte("peek-buried\r\n")
	return jackd.responseJobChunkWithContext(ctx, command, "FOUND", []string{NotFound})
}

func (jackd *Client) StatsJob(ctx context.Context, id uint32) (*JobStats, error) {
	var stats JobStats
	err := jackd.executeCommandWithYAMLResponse(
		ctx,
		[]byte(fmt.Sprintf("stats-job %d\r\n", id)),
		&stats,
		[]string{NotFound},
	)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (jackd *Client) StatsTube(ctx context.Context, tubeName string) (*TubeStats, error) {
	var stats TubeStats
	err := jackd.executeCommandWithYAMLResponse(
		ctx,
		[]byte(fmt.Sprintf("stats-tube %s\r\n", tubeName)),
		&stats,
		[]string{NotFound},
	)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (jackd *Client) Stats(ctx context.Context) (*SystemStats, error) {
	var stats SystemStats
	err := jackd.executeCommandWithYAMLResponse(
		ctx,
		[]byte("stats\r\n"),
		&stats,
		[]string{NotFound},
	)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (jackd *Client) ListTubes(ctx context.Context) (*TubeList, error) {
	var tubes TubeList
	err := jackd.executeCommandWithYAMLResponse(
		ctx,
		[]byte("list-tubes\r\n"),
		&tubes,
		[]string{NotFound},
	)
	if err != nil {
		return nil, err
	}
	return &tubes, nil
}

var Delimiter = []byte("\r\n")
var MaxTubeName = 200

func (jackd *Client) Reserve(ctx context.Context) (uint32, []byte, error) {
	command := []byte("reserve\r\n")
	return jackd.responseJobChunkWithContext(ctx, command, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (uint32, []byte, error) {
	command := []byte(fmt.Sprintf("reserve-with-timeout %d\r\n", uint32(timeout.Seconds())))
	return jackd.responseJobChunkWithContext(ctx, command, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveJob(ctx context.Context, job uint32) (uint32, []byte, error) {
	command := []byte(fmt.Sprintf("reserve-job %d\r\n", job))
	return jackd.responseJobChunkWithContext(ctx, command, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ListTubeUsed(ctx context.Context) (tube string, err error) {
	return jackd.executeCommandWithStringResponse(ctx, []byte("list-tube-used\r\n"), "USING %s", NoErrs)
}

func (jackd *Client) ListTubesWatched(ctx context.Context) (*TubeList, error) {
	var tubes TubeList
	err := jackd.executeCommandWithYAMLResponse(
		ctx,
		[]byte("list-tubes-watched\r\n"),
		&tubes,
		[]string{NotFound},
	)
	if err != nil {
		return nil, err
	}
	return &tubes, nil
}

func (jackd *Client) Quit(ctx context.Context) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	// Set deadline based on context using helper
	if err := setDeadlineFromContext(ctx, jackd.conn); err != nil {
		return err
	}
	// Ensure deadline is cleared when the function returns
	defer jackd.conn.SetDeadline(time.Time{})

	// Check context before potentially blocking write
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := jackd.write([]byte("quit\r\n")); err != nil {
		return checkContextError(ctx, err)
	}

	return jackd.conn.Close()
}

func (jackd *Client) expectedResponse(expected string, errs []string) error {
	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err := validate(resp, errs); err != nil {
			return err
		}

		if resp != expected {
			return unexpectedResponseError(resp)
		}
	}

	return nil
}

func (jackd *Client) responseJobChunk(expected string, errs []string) (uint32, []byte, error) {
	var id uint32 = 0
	var payloadLength = 0

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err := validate(resp, errs); err != nil {
			return 0, nil, err
		}

		parsed, err := fmt.Sscanf(resp, expected+" %d %d", &id, &payloadLength)
		if err != nil {
			return 0, nil, err
		}
		if parsed != 2 {
			return 0, nil, unexpectedResponseError(resp)
		}
	}

	if err := jackd.scanner.Err(); err != nil {
		return 0, nil, err
	}

	body := []byte("")

	for jackd.scanner.Scan() {
		body = append(body, jackd.scanner.Bytes()...)
		if len(body) == payloadLength {
			break
		}
		body = append(body, Delimiter...)
	}

	return id, body, jackd.scanner.Err()
}

func Must(client *Client, err error) *Client {
	if err != nil {
		log.Printf("unable to connect to beanstalkd instance %v", err)
		panic(err)
	}

	return client
}

func (jackd *Client) responseDataChunk(errs []string) ([]byte, error) {
	var payloadLength = 0

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err := validate(resp, errs); err != nil {
			return nil, err
		}

		parsed, err := fmt.Sscanf(resp, "OK %d", &payloadLength)
		if err != nil {
			return nil, err
		}
		if parsed != 1 {
			return nil, unexpectedResponseError(resp)
		}
	}

	if err := jackd.scanner.Err(); err != nil {
		return nil, err
	}

	body := []byte("")

	for jackd.scanner.Scan() {
		body = append(body, jackd.scanner.Bytes()...)
		if len(body) == payloadLength {
			break
		}
		body = append(body, Delimiter...)
	}

	return body, jackd.scanner.Err()
}

var unexpectedResponseError = func(resp string) error {
	return fmt.Errorf("unexpected response: %s", resp)
}

// Tweaked version of https://stackoverflow.com/a/37531472/1603399 that doesn't drop data
func splitCRLF(buf []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(buf) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(buf, Delimiter); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, buf[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(buf), buf, nil
	}
	// Request more data.
	return 0, nil, nil
}

func (jackd *Client) write(command []byte) error {
	if _, err := jackd.buffer.Write(command); err != nil {
		return err
	}
	if err := jackd.buffer.Flush(); err != nil {
		return err
	}
	return nil
}

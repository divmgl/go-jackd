package jackd

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

var Delimiter = []byte("\r\n")
var MaxTubeName = 200

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
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

func (jackd *Client) Put(body []byte, opts PutOpts) (uint32, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	command := []byte(fmt.Sprintf(
		"put %d %d %d %d\r\n",
		opts.Priority,
		uint(opts.Delay.Seconds()),
		uint(opts.TTR.Seconds()),
		len(body)),
	)

	// Write the command
	if _, err := jackd.buffer.Write(command); err != nil {
		return 0, err
	}
	// Write the body
	if _, err := jackd.buffer.Write(body); err != nil {
		return 0, err
	}
	// Write the delimiter
	if _, err := jackd.buffer.Write(Delimiter); err != nil {
		return 0, err
	}
	// Flush the writer
	if err := jackd.buffer.Flush(); err != nil {
		return 0, err
	}

	if jackd.scanner.Scan() {
		resp := string(jackd.scanner.Bytes())
		if err := validate(resp, []string{
			Buried,
			ExpectedCRLF,
			JobTooBig,
			Draining,
		}); err != nil {
			return 0, err
		}

		var id uint32 = 0

		_, err := fmt.Sscanf(resp, "INSERTED %d", &id)
		if err != nil {
			_, err = fmt.Sscanf(resp, "BURIED %d", &id)
			if err != nil {
				return id, ErrBuried
			}
		}

		return id, nil
	}

	return 0, jackd.scanner.Err()
}

func (jackd *Client) Use(tube string) (usingTube string, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write([]byte(fmt.Sprintf("use %s\r\n", tube))); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, NoErrs); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, "USING %s", &usingTube)
		if err != nil {
			return
		}
	}

	err = jackd.scanner.Err()
	return
}

func (jackd *Client) Kick(numJobs uint32) (kicked uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = jackd.write([]byte(fmt.Sprintf("kick %d\r\n", numJobs))); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, NoErrs); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, "KICKED %d", &kicked)
		if err != nil {
			return
		}
	}

	err = jackd.scanner.Err()
	return
}

func (jackd *Client) KickJob(id uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("kick-job %d\r\n", id))); err != nil {
		return err
	}

	return jackd.expectedResponse("KICKED", []string{NotFound})
}

func (jackd *Client) Delete(job uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("delete %d\r\n", job))); err != nil {
		return err
	}

	return jackd.expectedResponse("DELETED", []string{NotFound})
}

func (jackd *Client) PauseTube(tube string, delay time.Duration) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf(
		"pause-tube %s %d\r\n",
		tube,
		uint32(delay.Seconds()),
	))); err != nil {
		return err
	}

	return jackd.expectedResponse("PAUSED", []string{NotFound})
}

func (jackd *Client) Release(job uint32, opts ReleaseOpts) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf(
		"release %d %d %d\r\n",
		job,
		opts.Priority,
		uint32(opts.Delay.Seconds()),
	))); err != nil {
		return err
	}

	return jackd.expectedResponse("RELEASED", []string{Buried, NotFound})
}

func (jackd *Client) Bury(job uint32, priority uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf(
		"bury %d %d\r\n",
		job,
		priority,
	))); err != nil {
		return err
	}

	return jackd.expectedResponse("BURIED", []string{NotFound})
}

func (jackd *Client) Touch(job uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("touch %d\r\n", job))); err != nil {
		return err
	}

	return jackd.expectedResponse("TOUCHED", []string{NotFound})
}

func (jackd *Client) Watch(tube string) (watched uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write([]byte(fmt.Sprintf("watch %s\r\n", tube))); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, NoErrs); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, "WATCHING %d", &watched)
		if err != nil {
			return
		}
	}

	err = jackd.scanner.Err()
	return
}

func (jackd *Client) Ignore(tube string) (watched uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write([]byte(fmt.Sprintf("ignore %s\r\n", tube))); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, []string{NotIgnored}); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, "WATCHING %d", &watched)
		if err != nil {
			return
		}
	}

	err = jackd.scanner.Err()
	return
}

func (jackd *Client) Reserve() (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("reserve\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveWithTimeout(timeout time.Duration) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	command := []byte(fmt.Sprintf("reserve-with-timeout %d\r\n", uint32(timeout.Seconds())))

	if err := jackd.write(command); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveJob(job uint32) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("reserve-job %d\r\n", job))); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) Peek(job uint32) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("peek %d\r\n", job))); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("FOUND", []string{NotFound})
}

func (jackd *Client) PeekReady() (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("peek-ready\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("FOUND", []string{NotFound})
}

func (jackd *Client) PeekDelayed() (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("peek-delayed\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("FOUND", []string{NotFound})
}

func (jackd *Client) PeekBuried() (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("peek-buried\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk("FOUND", []string{NotFound})
}

func (jackd *Client) StatsJob(id uint32) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("stats-job %d\r\n", id))); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk([]string{NotFound})
}

func (jackd *Client) StatsTube(tubeName string) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte(fmt.Sprintf("stats-tube %s\r\n", tubeName))); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk([]string{NotFound})
}

func (jackd *Client) Stats() ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("stats\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk([]string{NotFound})
}

func (jackd *Client) ListTubes() ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("list-tubes\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk([]string{NotFound})
}

func (jackd *Client) ListTubeUsed() (tube string, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = jackd.write([]byte("list-tube-used\r\n")); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, NoErrs); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, "USING %s", &tube)
		if err != nil {
			return
		}
	}

	err = jackd.scanner.Err()
	return
}

func (jackd *Client) ListTubesWatched() ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("list-tubes-watched\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk([]string{NotFound})
}

func (jackd *Client) Quit() error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write([]byte("quit\r\n")); err != nil {
		return err
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
		log.Fatalf("unable to connect to beanstalkd instance %v", err)
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

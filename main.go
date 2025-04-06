package jackd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

var Delimiter = []byte("\r\n")
var MaxTubeName = 200

func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
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
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	command := []byte(fmt.Sprintf(
		"put %d %d %d %d\r\n",
		opts.Priority,
		uint(opts.Delay.Seconds()),
		uint(opts.TTR.Seconds()),
		len(body)),
	)

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetWriteDeadline(deadline)
		defer jackd.conn.SetWriteDeadline(time.Time{})
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if _, err := jackd.buffer.Write(command); err != nil {
		return 0, err
	}
	if _, err := jackd.buffer.Write(body); err != nil {
		return 0, err
	}
	if _, err := jackd.buffer.Write(Delimiter); err != nil {
		return 0, err
	}
	if err := jackd.buffer.Flush(); err != nil {
		return 0, err
	}

	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		if !jackd.scanner.Scan() {
			err := jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return 0, err
		}
	}

	resp := string(jackd.scanner.Bytes())
	if err := validate(resp, []string{
		Buried,
		ExpectedCRLF,
		JobTooBig,
		Draining,
	}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, err
	}

	var id uint32 = 0

	_, err := fmt.Sscanf(resp, "INSERTED %d", &id)
	if err != nil {
		_, err = fmt.Sscanf(resp, "BURIED %d", &id)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return id, ctxErr
			}
			return id, ErrBuried
		}
	}

	return id, nil
}

func (jackd *Client) Use(ctx context.Context, tube string) (usingTube string, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write(ctx, []byte(fmt.Sprintf("use %s\r\n", tube))); err != nil {
		return
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err = validate(resp, NoErrs); err != nil {
				return
			}

			_, err = fmt.Sscanf(resp, "USING %s", &usingTube)
			if err != nil {
				return
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return
		}
	}
	if err == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
	}

	return
}

func (jackd *Client) Kick(ctx context.Context, numJobs uint32) (kicked uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = jackd.write(ctx, []byte(fmt.Sprintf("kick %d\r\n", numJobs))); err != nil {
		return
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err = validate(resp, NoErrs); err != nil {
				return
			}

			_, err = fmt.Sscanf(resp, "KICKED %d", &kicked)
			if err != nil {
				return
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return
		}
	}
	if err == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
	}

	return
}

func (jackd *Client) KickJob(ctx context.Context, id uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("kick-job %d\r\n", id))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "KICKED", []string{NotFound})
}

func (jackd *Client) Delete(ctx context.Context, job uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("delete %d\r\n", job))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "DELETED", []string{NotFound})
}

func (jackd *Client) PauseTube(ctx context.Context, tube string, delay time.Duration) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := validateTubeName(tube); err != nil {
		return err
	}

	if err := jackd.write(ctx, []byte(fmt.Sprintf(
		"pause-tube %s %d\r\n",
		tube,
		uint32(delay.Seconds()),
	))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "PAUSED", []string{NotFound})
}

func (jackd *Client) Release(ctx context.Context, job uint32, opts ReleaseOpts) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf(
		"release %d %d %d\r\n",
		job,
		opts.Priority,
		uint32(opts.Delay.Seconds()),
	))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "RELEASED", []string{Buried, NotFound})
}

func (jackd *Client) Bury(ctx context.Context, job uint32, priority uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf(
		"bury %d %d\r\n",
		job,
		priority,
	))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "BURIED", []string{NotFound})
}

func (jackd *Client) Touch(ctx context.Context, job uint32) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("touch %d\r\n", job))); err != nil {
		return err
	}

	return jackd.expectedResponse(ctx, "TOUCHED", []string{NotFound})
}

func (jackd *Client) Watch(ctx context.Context, tube string) (watched uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write(ctx, []byte(fmt.Sprintf("watch %s\r\n", tube))); err != nil {
		return
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err = validate(resp, NoErrs); err != nil {
				return
			}

			_, err = fmt.Sscanf(resp, "WATCHING %d", &watched)
			if err != nil {
				return
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return
		}
	}
	if err == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
	}

	return
}

func (jackd *Client) Ignore(ctx context.Context, tube string) (watched uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = validateTubeName(tube); err != nil {
		return
	}

	if err = jackd.write(ctx, []byte(fmt.Sprintf("ignore %s\r\n", tube))); err != nil {
		return
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err = validate(resp, []string{NotIgnored}); err != nil {
				return
			}

			_, err = fmt.Sscanf(resp, "WATCHING %d", &watched)
			if err != nil {
				return
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return
		}
	}
	if err == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
	}

	return
}

func (jackd *Client) Reserve(ctx context.Context) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("reserve\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveWithTimeout(ctx context.Context, timeout time.Duration) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	ctxTimeout := ctx

	reqDeadline := time.Now().Add(timeout)
	ctxDeadline, hasCtxDeadline := ctx.Deadline()

	if hasCtxDeadline && ctxDeadline.Before(reqDeadline) {
		// Context deadline is sooner, use the original context.
	} else {
		var cancel context.CancelFunc
		ctxTimeout, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := jackd.write(ctxTimeout, []byte(fmt.Sprintf("reserve-with-timeout %d\r\n", uint32(timeout.Seconds())))); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return 0, nil, ctxErr
		}
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctxTimeout, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) ReserveJob(ctx context.Context, job uint32) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("reserve-job %d\r\n", job))); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "RESERVED", []string{DeadlineSoon, TimedOut})
}

func (jackd *Client) Peek(ctx context.Context, job uint32) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("peek %d\r\n", job))); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekReady(ctx context.Context) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("peek-ready\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekDelayed(ctx context.Context) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("peek-delayed\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "FOUND", []string{NotFound})
}

func (jackd *Client) PeekBuried(ctx context.Context) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("peek-buried\r\n")); err != nil {
		return 0, nil, err
	}

	return jackd.responseJobChunk(ctx, "FOUND", []string{NotFound})
}

func (jackd *Client) StatsJob(ctx context.Context, id uint32) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte(fmt.Sprintf("stats-job %d\r\n", id))); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk(ctx, []string{NotFound})
}

func (jackd *Client) StatsTube(ctx context.Context, tubeName string) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := validateTubeName(tubeName); err != nil {
		return nil, err
	}

	if err := jackd.write(ctx, []byte(fmt.Sprintf("stats-tube %s\r\n", tubeName))); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk(ctx, []string{NotFound})
}

func (jackd *Client) Stats(ctx context.Context) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("stats\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk(ctx, []string{NotFound})
}

func (jackd *Client) ListTubes(ctx context.Context) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("list-tubes\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk(ctx, []string{NotFound})
}

func (jackd *Client) ListTubeUsed(ctx context.Context) (tube string, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err = jackd.write(ctx, []byte("list-tube-used\r\n")); err != nil {
		return
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err = validate(resp, NoErrs); err != nil {
				return
			}

			_, err = fmt.Sscanf(resp, "USING %s", &tube)
			if err != nil {
				return
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			return
		}
	}
	if err == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
	}

	return
}

func (jackd *Client) ListTubesWatched(ctx context.Context) ([]byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("list-tubes-watched\r\n")); err != nil {
		return nil, err
	}

	return jackd.responseDataChunk(ctx, []string{NotFound})
}

func (jackd *Client) Quit(ctx context.Context) error {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	if err := jackd.write(ctx, []byte("quit\r\n")); err != nil {
		_ = jackd.conn.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}

	return jackd.conn.Close()
}

func (jackd *Client) expectedResponse(ctx context.Context, expected string, errs []string) error {
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err := validate(resp, errs); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return err
			}

			if resp != expected {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return unexpectedResponseError(resp)
			}
		} else {
			err := jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
	}
	if ctx.Err() == nil && !jackd.scanner.Scan() {
		err := jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
		return err
	}

	return nil
}

func (jackd *Client) responseJobChunk(ctx context.Context, expected string, errs []string) (uint32, []byte, error) {
	var id uint32 = 0
	var payloadLength = 0
	var err error
	var body []byte

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err := validate(resp, errs); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return 0, nil, ctxErr
				}
				return 0, nil, err
			}

			parsed, err := fmt.Sscanf(resp, expected+" %d %d", &id, &payloadLength)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return 0, nil, ctxErr
				}
				return 0, nil, err
			}
			if parsed != 2 {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return 0, nil, ctxErr
				}
				return 0, nil, unexpectedResponseError(resp)
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return id, nil, ctxErr
			}
			return id, nil, err
		}
	}
	if ctx.Err() == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil && len(body) < payloadLength {
			err = fmt.Errorf("connection closed prematurely while reading job body: read %d, expected %d", len(body), payloadLength)
		} else if err == nil {
			err = net.ErrClosed
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return id, body, ctxErr
		}
		return id, body, err
	}

	body = make([]byte, 0, payloadLength+len(Delimiter))

	for {
		select {
		case <-ctx.Done():
			return id, body, ctx.Err()
		default:
			if !jackd.scanner.Scan() {
				err = jackd.scanner.Err()
				if err == nil && len(body) < payloadLength {
					err = fmt.Errorf("connection closed prematurely while reading job body: read %d, expected %d", len(body), payloadLength)
				} else if err == nil {
					err = net.ErrClosed
				}
				if ctxErr := ctx.Err(); ctxErr != nil {
					return id, body, ctxErr
				}
				return id, body, err
			}
		}

		scannedBytes := jackd.scanner.Bytes()
		body = append(body, scannedBytes...)

		if len(body) >= payloadLength {
			if len(body) > payloadLength {
				body = body[:payloadLength]
			}
			break
		}
		body = append(body, Delimiter...)
	}

	if err = ctx.Err(); err != nil {
		return id, body, err
	}
	if err == nil {
		err = jackd.scanner.Err()
	}
	if err != nil {
		return id, body, err
	}

	return id, body, nil
}

func (jackd *Client) responseDataChunk(ctx context.Context, errs []string) ([]byte, error) {
	var payloadLength = 0
	var err error

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetReadDeadline(deadline)
		defer jackd.conn.SetReadDeadline(time.Time{})
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err := validate(resp, errs); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				return nil, err
			}

			parsed, err := fmt.Sscanf(resp, "OK %d", &payloadLength)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				return nil, err
			}
			if parsed != 1 {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				return nil, unexpectedResponseError(resp)
			}
		} else {
			err = jackd.scanner.Err()
			if err == nil {
				err = net.ErrClosed
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, err
		}
	}
	if ctx.Err() == nil && !jackd.scanner.Scan() {
		err = jackd.scanner.Err()
		if err == nil {
			err = net.ErrClosed
		}
		return nil, err
	}

	body := make([]byte, 0, payloadLength+len(Delimiter))

	for {
		select {
		case <-ctx.Done():
			return body, ctx.Err()
		default:
			if !jackd.scanner.Scan() {
				err = jackd.scanner.Err()
				if err == nil && len(body) < payloadLength {
					err = fmt.Errorf("connection closed prematurely while reading data chunk: read %d, expected %d", len(body), payloadLength)
				} else if err == nil {
					err = net.ErrClosed
				}
				if ctxErr := ctx.Err(); ctxErr != nil {
					return body, ctxErr
				}
				return body, err
			}
		}

		scannedBytes := jackd.scanner.Bytes()
		body = append(body, scannedBytes...)

		if len(body) >= payloadLength {
			if len(body) > payloadLength {
				body = body[:payloadLength]
			}
			break
		}
		body = append(body, Delimiter...)
	}

	if err = ctx.Err(); err != nil {
		return body, err
	}
	if err == nil {
		err = jackd.scanner.Err()
	}
	if err != nil {
		return body, err
	}

	return body, nil
}

var unexpectedResponseError = func(resp string) error {
	return fmt.Errorf("unexpected response: %s", resp)
}

func splitCRLF(buf []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(buf) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(buf, Delimiter); i >= 0 {
		return i + 2, buf[0:i], nil
	}
	if atEOF {
		return len(buf), buf, nil
	}
	return 0, nil, nil
}

func (jackd *Client) write(ctx context.Context, command []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		jackd.conn.SetWriteDeadline(deadline)
		defer jackd.conn.SetWriteDeadline(time.Time{})
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if _, err := jackd.buffer.Write(command); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := jackd.buffer.Flush(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	return nil
}

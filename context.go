package jackd

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/goccy/go-yaml"
)

// Helper function to set the connection deadline based on context
func setDeadlineFromContext(ctx context.Context, conn net.Conn) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set deadline: %w", err)
		}
	} else {
		// Clear potentially pre-existing deadline if context has none
		if err := conn.SetDeadline(time.Time{}); err != nil {
			return fmt.Errorf("failed to clear deadline: %w", err)
		}
	}
	return nil
}

// Helper function to check for context errors, particularly after a network timeout
func checkContextError(ctx context.Context, opErr error) error {
	if opErr == nil {
		return nil // No operation error
	}

	// Check if the operation error is a network timeout
	if netErr, ok := opErr.(net.Error); ok && netErr.Timeout() {
		// If it's a timeout, check if the context deadline was also exceeded
		if ctxErr := ctx.Err(); ctxErr == context.DeadlineExceeded {
			return ctxErr // Return the context error
		}
	}

	// Otherwise, return the original operation error
	return opErr
}

func (jackd *Client) expectedResponseWithContext(ctx context.Context, expected string, errs []string) error {
	done := make(chan error, 1)

	go func() {
		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err := validate(resp, errs); err != nil {
				done <- err
				return
			}

			if resp != expected {
				done <- unexpectedResponseError(resp)
				return
			}
		}
		done <- jackd.scanner.Err()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// executeCommand executes a beanstalkd command with context awareness
func (jackd *Client) executeCommand(ctx context.Context, command []byte, expectedResponse string, allowedErrors []string) error {
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

	if err := jackd.write(command); err != nil {
		return checkContextError(ctx, err)
	}

	// Check context before potentially blocking read
	if err := ctx.Err(); err != nil {
		return err
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err := validate(resp, allowedErrors); err != nil {
			return err
		}

		if resp != expectedResponse {
			return unexpectedResponseError(resp)
		}
	}

	return checkContextError(ctx, jackd.scanner.Err())
}

// executeCommandWithUint32Response executes a command and scans a uint32 from the response
func (jackd *Client) executeCommandWithUint32Response(ctx context.Context, command []byte, responseFormat string, allowedErrors []string) (value uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	// Set deadline based on context using helper
	if err = setDeadlineFromContext(ctx, jackd.conn); err != nil {
		return 0, err
	}
	// Ensure deadline is cleared when the function returns
	defer jackd.conn.SetDeadline(time.Time{})

	// Check context before potentially blocking write
	if err = ctx.Err(); err != nil {
		return 0, err
	}

	if err = jackd.write(command); err != nil {
		return 0, checkContextError(ctx, err)
	}

	// Check context before potentially blocking read
	if err = ctx.Err(); err != nil {
		return 0, err
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, allowedErrors); err != nil {
			return 0, err
		}

		_, err = fmt.Sscanf(resp, responseFormat, &value)
		if err != nil {
			return 0, err
		}
	}

	err = checkContextError(ctx, jackd.scanner.Err())
	return
}

func (jackd *Client) responseJobChunkWithContext(ctx context.Context, command []byte, expected string, errs []string) (uint32, []byte, error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	// Check context before starting
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}

	if err := jackd.write(command); err != nil {
		return 0, nil, checkContextError(ctx, err)
	}

	// Create channels for results and errors
	type scanResult struct {
		id      uint32
		payload []byte
		err     error
	}
	resultCh := make(chan scanResult, 1)
	doneCh := make(chan struct{})
	defer close(doneCh)

	// Start a goroutine to handle the scanning
	go func() {
		var id uint32 = 0
		var payloadLength = 0

		if jackd.scanner.Scan() {
			resp := jackd.scanner.Text()
			if err := validate(resp, errs); err != nil {
				resultCh <- scanResult{err: err}
				return
			}

			parsed, err := fmt.Sscanf(resp, expected+" %d %d", &id, &payloadLength)
			if err != nil {
				resultCh <- scanResult{err: err}
				return
			}
			if parsed != 2 {
				resultCh <- scanResult{err: unexpectedResponseError(resp)}
				return
			}
		}

		if err := jackd.scanner.Err(); err != nil {
			resultCh <- scanResult{err: err}
			return
		}

		body := []byte("")

		for jackd.scanner.Scan() {
			select {
			case <-doneCh:
				return
			default:
				body = append(body, jackd.scanner.Bytes()...)
				if len(body) == payloadLength {
					resultCh <- scanResult{id: id, payload: body}
					return
				}
				body = append(body, Delimiter...)
			}
		}

		if err := jackd.scanner.Err(); err != nil {
			resultCh <- scanResult{err: err}
			return
		}

		resultCh <- scanResult{id: id, payload: body}
	}()

	// Wait for either the scan to complete or context to be cancelled
	select {
	case result := <-resultCh:
		return result.id, result.payload, result.err
	case <-ctx.Done():
		// Instead of closing the connection, we'll signal the goroutine to stop
		// and let the scanner timeout naturally
		return 0, nil, ctx.Err()
	}
}

// executeCommandWithYAMLResponse executes a command and unmarshals the YAML response into the provided value
func (jackd *Client) executeCommandWithYAMLResponse(ctx context.Context, command []byte, v interface{}, allowedErrors []string) error {
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

	if err := jackd.write(command); err != nil {
		return checkContextError(ctx, err)
	}

	// Check context before potentially blocking read
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := jackd.responseDataChunk(allowedErrors)
	if err != nil {
		return checkContextError(ctx, err)
	}

	if err := yaml.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to parse YAML response: %w", err)
	}

	return nil
}

func (jackd *Client) executeCommandWithStringResponse(ctx context.Context, command []byte, responseFormat string, allowedErrors []string) (value string, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	// Set deadline based on context using helper
	if err = setDeadlineFromContext(ctx, jackd.conn); err != nil {
		return
	}
	// Ensure deadline is cleared when the function returns
	defer jackd.conn.SetDeadline(time.Time{})

	// Check context before potentially blocking write
	if err = ctx.Err(); err != nil {
		return
	}

	if err = jackd.write(command); err != nil {
		// Check context error using helper
		err = checkContextError(ctx, err)
		return
	}

	// Check context before potentially blocking read
	if err = ctx.Err(); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, allowedErrors); err != nil {
			return
		}

		_, err = fmt.Sscanf(resp, responseFormat, &value)
		if err != nil {
			return
		}
	}

	// Check scanner error after Scan() returns false, potentially due to timeout
	err = checkContextError(ctx, jackd.scanner.Err())
	return
}

// executeCommandWithPutResponse executes a command and handles the unique response format of the Put command
func (jackd *Client) executeCommandWithPutResponse(ctx context.Context, command []byte, body []byte, allowedErrors []string) (id uint32, err error) {
	jackd.mutex.Lock()
	defer jackd.mutex.Unlock()

	// Set deadline based on context using helper
	if err = setDeadlineFromContext(ctx, jackd.conn); err != nil {
		return
	}
	// Ensure deadline is cleared when the function returns
	defer jackd.conn.SetDeadline(time.Time{})

	// Check context before potentially blocking write
	if err = ctx.Err(); err != nil {
		return
	}

	// Write the command
	if _, err = jackd.buffer.Write(command); err != nil {
		return 0, checkContextError(ctx, err)
	}
	// Write the body
	if _, err = jackd.buffer.Write(body); err != nil {
		return 0, checkContextError(ctx, err)
	}
	// Write the delimiter
	if _, err = jackd.buffer.Write(Delimiter); err != nil {
		return 0, checkContextError(ctx, err)
	}
	// Flush the writer
	if err = jackd.buffer.Flush(); err != nil {
		return 0, checkContextError(ctx, err)
	}

	// Check context before potentially blocking read
	if err = ctx.Err(); err != nil {
		return
	}

	if jackd.scanner.Scan() {
		resp := jackd.scanner.Text()
		if err = validate(resp, allowedErrors); err != nil {
			return 0, err
		}

		_, err = fmt.Sscanf(resp, "INSERTED %d", &id)
		if err != nil {
			_, err = fmt.Sscanf(resp, "BURIED %d", &id)
			if err != nil {
				return 0, err
			}
			return id, ErrBuried
		}
	}

	err = checkContextError(ctx, jackd.scanner.Err())
	return
}

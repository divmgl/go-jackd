# go-jackd

[![Go Tests](https://github.com/divmgl/go-jackd/actions/workflows/test.yml/badge.svg)](https://github.com/divmgl/go-jackd/actions/workflows/test.yml)


```go
package main

import (
	"context"
	"log"
	"github.com/getjackd/go-jackd"
)

func main() {
	ctx := context.Background()
	conn := jackd.Must(jackd.Dial(ctx, "localhost:11300")) // => *jackd.Client

	// Producing
	conn.Put(ctx, []byte("Hello!"), jackd.DefaultPutOpts())

	// Consuming
	id, payload, err := conn.Reserve(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Process the job then delete it
	conn.Delete(ctx, id)
}
```

## Why

`go-jackd` was inspired by the original `jackd` client made for Node.js and it follows the same principles:

- Concise and easy to use API
- No dependencies
- Protocol accuracy/completeness

Most, if not all, Go `beanstalkd` clients are out of date (`gobeanstalk`, `gostalk`), are missing features (`go-beanstalk` does not have the `reserve` command) and some don't handle certain use-cases, such as:

- Job payloads that are bigger than the current TCP frame
- Line breaks in job payloads that match the `beanstalkd` delimiter

## Overview

`beanstalkd` is a simple and blazing fast work queue. Producers connected through TCP sockets send in jobs to be processed at a later time by a consumer.

If you don't have experience using `beanstalkd`, [it's a good idea to read the `beanstalkd` protocol before using this library.](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt)

### Connecting and disconnecting

```go
ctx := context.Background()
conn, err := jackd.Dial(ctx, "localhost:11300")
if err != nil {
	// Handle error
}

// If you just want to fail in the case of any error, such as when your
// code simply won't work if a connection to beanstalkd can't be made:
conn := jackd.Must(jackd.Dial(ctx, "localhost:11300"))

// Disconnect
conn.Quit(ctx)
```

### Context Support

`go-jackd` provides first-class support for Go's context package. All operations accept a context parameter which can be used for:

- Cancellation: Cancel long-running operations like `Reserve`
- Deadlines: Set timeouts for operations
- Values: Pass request-scoped values

Example with timeout:
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// This will timeout after 5 seconds if no job is available
id, payload, err := conn.Reserve(ctx)
if err == context.DeadlineExceeded {
	// Handle timeout
}
```

### Producers

#### Adding jobs to a tube

You can add jobs to a tube by using the `put` command, which accepts a payload and returns a job ID.

```go
conn.Put(ctx, []byte("my long running job"), jackd.DefaultPutOpts())
```

All jobs sent to `beanstalkd` have a priority, a delay, and TTR (time-to-run) specification. `jackd` provides a `DefaultPutOpts()` function that returns a new `PutOpts` struct with `0` priority, `0` delay, and `60` TTR, which means consumers will have 60 seconds to finish the job after reservation. You can override these defaults:

```go
opts := PutOpts{
	Priority: 0,
	Delay: 2 * time.Minute, // delays for two minutes
	TTR: 10 * time.Minute, // jobs can be reserved for ten minutes
}

conn.Put(ctx, []byte("delayed job"), opts)
```

Jobs with lower priorities are handled first. Refer to [the protocol specs](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt#L126) for more information on job options.

#### Using different tubes

All jobs are added to the `default` tube by default. You can change the tube to send jobs to with `use`.

```go
tubeName, err := conn.Use(ctx, "awesome-tube") // tubeName == "awesome-tube"
conn.Put(ctx, []byte("awesome job"), jackd.DefaultPutOpts()) // job is put into awesome-tube
```

### Consumers

#### Reserving a job

Consumers work by reserving jobs in a tube. Reserving is a blocking operation and execution will stop until a job has been reserved or the context is cancelled.

```go
id, payload, err := conn.Reserve(ctx) // Reserve the next job
```

#### Reserving specific jobs (1.12+)

You can also reserve specific jobs as of `beanstalkd` 1.12. This command will simply fail in older versions.

```go
id, payload, err := conn.PeekReady(ctx) // PeekReady returns the payload
_, _, err := conn.ReserveJob(ctx, id)
```

#### Performing job operations

Once you've reserved a job, there are several operations you can perform on it. The most common operation will be deleting the job after the consumer is finished processing it.

```go
err := conn.Delete(ctx, id)
```

Consumers can also give up their reservation by releasing the job. You'll usually want to release the job if an error occurred on the consumer and you want to put it back in the queue immediately.

```go
// Release immediately with high priority (0) and no delay (0)
err := conn.Release(ctx, id, jackd.DefaultReleaseOpts())

// Release with a priority and delay
opts := ReleaseOpts{
	Priority: 10,
	Delay: 10 * time.Second // release after 10 seconds
}
err := conn.Release(ctx, id, opts)
```

However, you may want to bury the job to be processed later under certain conditions, such as a recurring error or a job that can't be processed. Buried jobs will not be processed until they are kicked.

```go
err := conn.Bury(ctx, id, /* priority */ 0)
// ...some time later...
err := conn.KickJob(ctx, id)
```

You'll notice that the kick operation is suffixed by `Job`. This is because there is a `kick` command in `beanstalkd` which will kick a certain number of jobs back into the tube.

```go
// beanstalkd will attempt to kick 100 jobs. The number of jobs kicked will be returned
numKicked, err := conn.Kick(ctx, 100)
```

Consumers will sometimes need additional time to run jobs. You can `touch` those jobs to let `beanstalkd` know you're still processing them.

```go
err := conn.Touch(ctx, id)
```

#### Watching on multiple tubes

By default, all consumers will watch the `default` tube only. Consumers can elect what tubes they want to watch.

```go
numWatched, err := conn.Watch(ctx, "my-special-tube")
```

If a consumer is watching a tube and it no longer needs it, you can choose to ignore that tube as well.

```go
numWatched, err := conn.Ignore(ctx, "default")
```

Please keep in mind that attempting to ignore the only tube being watched will result in an error.

You can also bring back the current tubes watched using `list-tubes-watched`. However, please keep in mind that this command returns YAML, so you'll need to parse it.

```go
import "github.com/goccy/go-yaml"

resp, err := conn.ListTubesWatched()

var tubes []string
err = yaml.Unmarshal(resp, &tubes)
```

### All commands are available

`go-jackd` has first class support for all `beanstalkd` commands. Please refer to the [`beanstalkd` protocol](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.txt) for a complete list of commands.

## Worker pattern

Here's an example implementation of a worker pattern with proper context handling:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/getjackd/go-jackd"
)

func main() {
	ctx := context.Background()
	conn := jackd.Must(jackd.Dial(ctx, "localhost:11300"))

	err := worker(ctx, conn, func(ctx context.Context, id uint32, body []byte) error {
		// ...process the job body...

		// ...then delete the job once we're finished.
		if err := conn.Delete(ctx, id); err != nil {
			return err
		}

		return nil
	})

	log.Fatal(err)
}

func worker(ctx context.Context, conn *jackd.Client, fn func(ctx context.Context, id uint32, body []byte) error) error {
	for {
		// Create a context with timeout for each reservation
		reserveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		id, body, err := conn.Reserve(reserveCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Timeout is expected, continue to next iteration
				continue
			}
			// If we're unable to reserve jobs, something's gone wrong. Quit
			// the consumer.
			return err
		}

		// Create a new context for job processing with the job's TTR
		jobCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if err := fn(jobCtx, id, body); err != nil {
			log.Printf("unable to process job %d: %+v", id, err)

			err = conn.Bury(jobCtx, id, 0)
			cancel()
			if err != nil {
				// If we're having issues burying errored jobs, something's gone
				// wrong. quit the consumer
				return err
			}
		}
		cancel()
	}
}
```

## Concurrency

`jackd` as of 1.1.0 supports issuing commands from multiple goroutines. In order to avoid concurrency issues, all `jackd` commands are synchronized with a mutex. This is because `beanstalkd` processes commands per connection serially. 

Please keep this in mind as your goroutines may block each other if they're utilizing the same `jackd` instance (especially with long-running commands, like the `reserve` commands). This is normally not a problem in most architectures, but if you do run into issues, you have several options:

* If you need to publish and consume from the same process, use two separate `jackd` instances: one for publishing and one for consuming 
* Ensure that you create individual `jackd` instances per goroutine. Keep in mind that this opens a new connection to `beanstalkd`.
* Keep all of your code synchronous when dealing with `jackd` (specifically, use mutexes, wait groups, or simply do not use multiple goroutines with `jackd`)

# License

MIT
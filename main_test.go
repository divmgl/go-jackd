package jackd_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	jackd "github.com/getjackd/go-jackd/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type JackdSuite struct {
	suite.Suite
	beanstalkd  *jackd.Client
	beanstalkd2 *jackd.Client
}

func TestJackdSuite(t *testing.T) {
	suite.Run(t, new(JackdSuite))
}

func (suite *JackdSuite) SetupTest() {
	beanstalkd, err := jackd.Dial(context.Background(), "localhost:11300")
	require.NoError(suite.T(), err)
	suite.beanstalkd = beanstalkd

	beanstalkd2, err := jackd.Dial(context.Background(), "localhost:11300")
	require.NoError(suite.T(), err)
	suite.beanstalkd2 = beanstalkd2
}

func (suite *JackdSuite) TearDownTest() {
	err := suite.beanstalkd.Quit(context.Background())
	require.NoError(suite.T(), err)
}

func TestConnects(t *testing.T) {
	beanstalkd, err := jackd.Dial(context.Background(), "localhost:11300")
	defer beanstalkd.Quit(context.Background())
	require.NoError(t, err)
}

func TestConnectsAndDisconnects(t *testing.T) {
	beanstalkd, err := jackd.Dial(context.Background(), "localhost:11300")
	require.NoError(t, err)
	err = beanstalkd.Quit(context.Background())
	require.NoError(t, err)
}

func (suite *JackdSuite) TestPutJob() {
	id, err := suite.beanstalkd.Put(context.Background(), []byte("test job"), jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)

	require.NoError(suite.T(), err)
	assert.IsType(suite.T(), uint32(0), id)
	assert.True(suite.T(), id > 0)
}

func (suite *JackdSuite) TestReserve() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(context.Background(), id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve(context.Background())
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestRelease() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve(context.Background())
	require.NoError(suite.T(), err)

	err = suite.beanstalkd.Release(context.Background(), reservedID, jackd.DefaultReleaseOpts())
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekReady(context.Background())
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeek() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.Peek(context.Background(), id)

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeekReady() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekReady(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeekDelayed() {
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	payload := []byte("test delayed job")

	id, err := suite.beanstalkd.Put(context.Background(), payload, opts)
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekDelayed(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestBury() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	// In order to bury a job, it must be reserved first
	_, _, err = suite.beanstalkd.ReserveJob(context.Background(), id)
	require.NoError(suite.T(), err)
	err = suite.beanstalkd.Bury(context.Background(), id, 0)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekBuried(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestKickBuried() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	// In order to bury a job, it must be reserved first
	_, _, err = suite.beanstalkd.ReserveJob(context.Background(), id)
	require.NoError(suite.T(), err)
	err = suite.beanstalkd.Bury(context.Background(), id, 0)
	require.NoError(suite.T(), err)

	kicked, err := suite.beanstalkd.Kick(context.Background(), 1)
	require.NoError(suite.T(), err)

	secondReservedID, secondReservedPayload, err := suite.beanstalkd.Reserve(context.Background())
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), uint32(1), kicked)
	assert.Equal(suite.T(), id, secondReservedID)
	assert.Equal(suite.T(), payload, secondReservedPayload)
}

func (suite *JackdSuite) TestReserveJob() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.ReserveJob(context.Background(), id)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestReserveWithTimeout() {
	// Test successful reserve
	payload := []byte("test job with timeout")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(context.Background(), id)

	reservedID, reservedPayload, err := suite.beanstalkd.ReserveWithTimeout(context.Background(), 1*time.Second)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)

	// Test timeout scenario
	_, _, err = suite.beanstalkd2.ReserveWithTimeout(context.Background(), 0*time.Second)
	assert.ErrorIs(suite.T(), err, jackd.ErrTimedOut)
}

func (suite *JackdSuite) TestReserveDelayedJob() {
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	payload := []byte("test delayed job")

	id, err := suite.beanstalkd.Put(context.Background(), payload, opts)
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(context.Background(), id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestHandlesJobsWithNewLines() {
	payload := []byte("test job\r\nwith line breaks")
	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(context.Background(), id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestHandlesMassiveJobs() {
	payload := make([]byte, 50000)
	_, err := rand.Read(payload)
	require.NoError(suite.T(), err)

	id, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve(context.Background())

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestUseTube() {
	tube := "some-other-tube"
	returnedTube, err := suite.beanstalkd.Use(context.Background(), tube)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), tube, returnedTube)
}

func (suite *JackdSuite) TestWatchTube() {
	tube := "some-other-tube"
	count, err := suite.beanstalkd.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), uint32(2), count)
}

func (suite *JackdSuite) TestListTubes() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)

	tubes, err := suite.beanstalkd2.ListTubes(context.Background())
	require.NoError(suite.T(), err)
	assert.Len(suite.T(), *tubes, 2)
}

func (suite *JackdSuite) TestListTubeUsed() {
	tube, err := suite.beanstalkd.ListTubeUsed(context.Background())
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), "default", tube)
}

func (suite *JackdSuite) TestListTubesWatched() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)
	_, err = suite.beanstalkd.Ignore(context.Background(), "default")
	require.NoError(suite.T(), err)

	tubes, err := suite.beanstalkd2.ListTubesWatched(context.Background())
	require.NoError(suite.T(), err)
	assert.Len(suite.T(), *tubes, 1)
}

func (suite *JackdSuite) TestPutReserveJobDifferentTube() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Use(context.Background(), tube)
	require.NoError(suite.T(), err)

	payload := []byte("my awesome other tube job")
	job, err := suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), job)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd2.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)
	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve(context.Background())
	defer suite.beanstalkd2.Delete(context.Background(), reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestIgnoresTube() {
	var defaultTubeJob, job uint32

	defaultTubeJobPayload := []byte("my default job")
	payload := []byte("my awesome other tube job")
	tube := "some-other-tube"

	defaultTubeJob, err := suite.beanstalkd.Put(context.Background(), defaultTubeJobPayload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), defaultTubeJob)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd.Use(context.Background(), tube)
	require.NoError(suite.T(), err)

	job, err = suite.beanstalkd.Put(context.Background(), payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), job)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd2.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)
	_, err = suite.beanstalkd2.Ignore(context.Background(), "default")
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd.Use(context.Background(), tube)
	require.NoError(suite.T(), err)

	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve(context.Background())
	defer suite.beanstalkd2.Delete(context.Background(), reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestPauseTube() {
	payload := []byte("my default job")
	pausedPayload := []byte("my awesome other tube job")
	tube := "some-other-tube"

	// Pause the other tube for five seconds
	err := suite.beanstalkd.PauseTube(context.Background(), tube, 5*time.Second)
	require.NoError(suite.T(), err)
	// Ask the second client to watch this paused tube
	_, err = suite.beanstalkd2.Watch(context.Background(), tube)
	require.NoError(suite.T(), err)

	// Put in a delayed job for one second in the default tube
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	job, err := suite.beanstalkd.Put(context.Background(), payload, opts)
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(context.Background(), job)

	// Put in a job with no delay in the paused tube. Jobs going into the paused
	// tube should have a delay now.
	_, err = suite.beanstalkd.Use(context.Background(), tube)
	require.NoError(suite.T(), err)
	pausedJob, err := suite.beanstalkd.Put(context.Background(), pausedPayload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(context.Background(), pausedJob)
	require.NoError(suite.T(), err)

	// The reserved job should be the one from the default payload and not the paused
	// tube job, even though that job has a 0 delay.
	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve(context.Background())
	defer suite.beanstalkd2.Delete(context.Background(), reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestReserveWithContextCancellation() {
	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will cancel the context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Try to reserve a job - this should block until the context is cancelled
	_, _, err := suite.beanstalkd.Reserve(ctx)

	// Verify that we got a context cancelled error
	assert.ErrorIs(suite.T(), err, context.Canceled)
}

func (suite *JackdSuite) TestReserveWithContextTimeout() {
	// Create a context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Try to reserve a job with a longer timeout than our context timeout
	_, _, err := suite.beanstalkd.ReserveWithTimeout(ctx, 5*time.Second)

	// Verify that the error is due to context deadline
	assert.ErrorIs(suite.T(), err, context.DeadlineExceeded)
}

func (suite *JackdSuite) TestPassingValuesViaContext() {
	// Create a workflow context with some metadata
	type workflowKey string
	const workflowIDKey workflowKey = "workflow-id"

	workflowCtx := context.WithValue(context.Background(), workflowIDKey, "workflow-123")

	// Put a job that will trigger another job
	payload := []byte("first job")
	firstJobID, err := suite.beanstalkd.Put(workflowCtx, payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(workflowCtx, firstJobID)

	// Reserve the first job
	reserveCtx := context.WithValue(workflowCtx, "current-job", firstJobID)
	id, body, err := suite.beanstalkd.Reserve(reserveCtx)
	require.NoError(suite.T(), err)

	// Verify we got the first job
	assert.Equal(suite.T(), firstJobID, id)
	assert.Equal(suite.T(), payload, body)

	// Get workflow ID from context
	workflowID, ok := reserveCtx.Value(workflowIDKey).(string)
	require.True(suite.T(), ok)
	assert.Equal(suite.T(), "workflow-123", workflowID)

	// Put a second job, passing along the workflow context
	secondPayload := []byte(fmt.Sprintf("second job in workflow %s", workflowID))
	secondJobID, err := suite.beanstalkd.Put(reserveCtx, secondPayload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(workflowCtx, secondJobID)

	// Reserve the second job
	id2, body2, err := suite.beanstalkd.Reserve(reserveCtx)
	require.NoError(suite.T(), err)

	// Verify the second job
	assert.Equal(suite.T(), secondJobID, id2)
	assert.Equal(suite.T(), secondPayload, body2)

	// Verify we can still access the workflow context
	workflowID2, ok := reserveCtx.Value(workflowIDKey).(string)
	require.True(suite.T(), ok)
	assert.Equal(suite.T(), "workflow-123", workflowID2)

	// Verify we can access the previous job ID
	previousJobID, ok := reserveCtx.Value("current-job").(uint32)
	require.True(suite.T(), ok)
	assert.Equal(suite.T(), firstJobID, previousJobID)
}

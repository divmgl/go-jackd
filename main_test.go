package jackd_test

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"

	"github.com/getjackd/go-jackd"
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
	beanstalkd, err := jackd.Dial("localhost:11300")
	require.NoError(suite.T(), err)
	suite.beanstalkd = beanstalkd

	beanstalkd2, err := jackd.Dial("localhost:11300")
	require.NoError(suite.T(), err)
	suite.beanstalkd2 = beanstalkd2
}

func (suite *JackdSuite) TearDownTest() {
	err := suite.beanstalkd.Quit()
	require.NoError(suite.T(), err)
}

func TestConnects(t *testing.T) {
	beanstalkd, err := jackd.Dial("localhost:11300")
	defer beanstalkd.Quit()
	require.NoError(t, err)
}

func TestConnectsAndDisconnects(t *testing.T) {
	beanstalkd, err := jackd.Dial("localhost:11300")
	require.NoError(t, err)
	err = beanstalkd.Quit()
	require.NoError(t, err)
}

func (suite *JackdSuite) TestPutJob() {
	id, err := suite.beanstalkd.Put([]byte("test job"), jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)

	require.NoError(suite.T(), err)
	assert.IsType(suite.T(), uint32(0), id)
	assert.True(suite.T(), id > 0)
}

func (suite *JackdSuite) TestReserve() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve()
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestRelease() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve()
	require.NoError(suite.T(), err)

	err = suite.beanstalkd.Release(reservedID, jackd.DefaultReleaseOpts())
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekReady()
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeek() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.Peek(id)

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeekReady() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekReady()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestPeekDelayed() {
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	payload := []byte("test delayed job")

	id, err := suite.beanstalkd.Put(payload, opts)
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekDelayed()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestBury() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	// In order to bury a job, it must be reserved first
	_, _, err = suite.beanstalkd.ReserveJob(id)
	require.NoError(suite.T(), err)
	err = suite.beanstalkd.Bury(id, 0)
	require.NoError(suite.T(), err)

	peekedId, peekedPayload, err := suite.beanstalkd.PeekBuried()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, peekedId)
	assert.Equal(suite.T(), payload, peekedPayload)
}

func (suite *JackdSuite) TestKickBuried() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	// In order to bury a job, it must be reserved first
	_, _, err = suite.beanstalkd.ReserveJob(id)
	require.NoError(suite.T(), err)
	err = suite.beanstalkd.Bury(id, 0)
	require.NoError(suite.T(), err)

	kicked, err := suite.beanstalkd.Kick(1)
	require.NoError(suite.T(), err)

	secondReservedID, secondReservedPayload, err := suite.beanstalkd.Reserve()
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), uint32(1), kicked)
	assert.Equal(suite.T(), id, secondReservedID)
	assert.Equal(suite.T(), payload, secondReservedPayload)
}

func (suite *JackdSuite) TestReserveJob() {
	payload := []byte("test job")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.ReserveJob(id)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestReserveWithTimeout() {
	// Test successful reserve
	payload := []byte("test job with timeout")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(id)

	reservedID, reservedPayload, err := suite.beanstalkd.ReserveWithTimeout(1 * time.Second)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)

	// Test timeout scenario
	_, _, err = suite.beanstalkd2.ReserveWithTimeout(0 * time.Second)
	assert.ErrorIs(suite.T(), err, jackd.ErrTimedOut)
}

func (suite *JackdSuite) TestReserveDelayedJob() {
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	payload := []byte("test delayed job")

	id, err := suite.beanstalkd.Put(payload, opts)
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestHandlesJobsWithNewLines() {
	payload := []byte("test job\r\nwith line breaks")
	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(id)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestHandlesMassiveJobs() {
	payload := make([]byte, 50000)
	_, err := rand.Read(payload)
	require.NoError(suite.T(), err)

	id, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(id)
	require.NoError(suite.T(), err)

	reservedID, reservedPayload, err := suite.beanstalkd.Reserve()

	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), id, reservedID)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestUseTube() {
	tube := "some-other-tube"
	returnedTube, err := suite.beanstalkd.Use(tube)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), tube, returnedTube)
}

func (suite *JackdSuite) TestWatchTube() {
	tube := "some-other-tube"
	count, err := suite.beanstalkd.Watch(tube)
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), uint32(2), count)
}

func (suite *JackdSuite) TestListTubes() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Watch(tube)
	require.NoError(suite.T(), err)

	resp, err := suite.beanstalkd2.ListTubes()
	require.NoError(suite.T(), err)

	var tubes []string
	err = yaml.Unmarshal(resp, &tubes)
	require.NoError(suite.T(), err)

	assert.Len(suite.T(), tubes, 2)
}

func (suite *JackdSuite) TestListTubeUsed() {
	tube, err := suite.beanstalkd.ListTubeUsed()
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), "default", tube)
}

func (suite *JackdSuite) TestListTubesWatched() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Watch(tube)
	require.NoError(suite.T(), err)
	_, err = suite.beanstalkd.Ignore("default")
	require.NoError(suite.T(), err)

	resp, err := suite.beanstalkd2.ListTubesWatched()
	require.NoError(suite.T(), err)

	var tubes []string
	err = yaml.Unmarshal(resp, &tubes)
	require.NoError(suite.T(), err)

	assert.Len(suite.T(), tubes, 1)
}

func (suite *JackdSuite) TestPutReserveJobDifferentTube() {
	tube := "some-other-tube"
	_, err := suite.beanstalkd.Use(tube)
	require.NoError(suite.T(), err)

	payload := []byte("my awesome other tube job")
	job, err := suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(job)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd2.Watch(tube)
	require.NoError(suite.T(), err)
	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve()
	defer suite.beanstalkd2.Delete(reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestIgnoresTube() {
	var defaultTubeJob, job uint32

	defaultTubeJobPayload := []byte("my default job")
	payload := []byte("my awesome other tube job")
	tube := "some-other-tube"

	defaultTubeJob, err := suite.beanstalkd.Put(defaultTubeJobPayload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(defaultTubeJob)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd.Use(tube)
	require.NoError(suite.T(), err)

	job, err = suite.beanstalkd.Put(payload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(job)
	require.NoError(suite.T(), err)

	_, err = suite.beanstalkd2.Watch(tube)
	require.NoError(suite.T(), err)
	_, err = suite.beanstalkd2.Ignore("default")
	require.NoError(suite.T(), err)

	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve()
	defer suite.beanstalkd2.Delete(reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

func (suite *JackdSuite) TestPauseTube() {
	payload := []byte("my default job")
	pausedPayload := []byte("my awesome other tube job")
	tube := "some-other-tube"

	// Pause the other tube for five seconds
	err := suite.beanstalkd.PauseTube(tube, 5*time.Second)
	require.NoError(suite.T(), err)
	// Ask the second client to watch this paused tube
	_, err = suite.beanstalkd2.Watch(tube)
	require.NoError(suite.T(), err)

	// Put in a delayed job for one second in the default tube
	opts := jackd.DefaultPutOpts()
	opts.Delay = 1 * time.Second
	job, err := suite.beanstalkd.Put(payload, opts)
	require.NoError(suite.T(), err)
	defer suite.beanstalkd.Delete(job)

	// Put in a job with no delay in the paused tube. Jobs going into the paused
	// tube should have a delay now.
	_, err = suite.beanstalkd.Use(tube)
	require.NoError(suite.T(), err)
	pausedJob, err := suite.beanstalkd.Put(pausedPayload, jackd.DefaultPutOpts())
	defer suite.beanstalkd.Delete(pausedJob)
	require.NoError(suite.T(), err)

	// The reserved job should be the one from the default payload and not the paused
	// tube job, even though that job has a 0 delay.
	reservedJob, reservedPayload, err := suite.beanstalkd2.Reserve()
	defer suite.beanstalkd2.Delete(reservedJob)
	require.NoError(suite.T(), err)

	assert.Equal(suite.T(), job, reservedJob)
	assert.Equal(suite.T(), payload, reservedPayload)
}

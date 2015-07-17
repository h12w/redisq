package redisq

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

type Job interface {
	Do() error
}

type JobQ struct {
	Name      string
	inputQ    *Q
	consumers []*Q
	doneQ     *Q
	failQ     *Q
	newJob    func() Job
	errCh     chan error
}

type JobQError struct {
	Msg string
	Job Job
	Err error
}

func (e *JobQError) Error() string {
	return e.Msg
}

func NewJobQ(name string, consumerCount int, newJob func() Job, pool *redis.Pool) (*JobQ, error) {
	q := &JobQ{
		Name:      name,
		inputQ:    New(name+":job", pool),
		consumers: make([]*Q, consumerCount),
		doneQ:     New(name+":done", pool),
		failQ:     New(name+":done", pool),
		newJob:    newJob,
		errCh:     make(chan error),
	}
	for i := range q.consumers {
		q.consumers[i] = New(name+":consumer:"+strconv.Itoa(i), pool)
	}
	// TODO: move working back to job queue
	return q, nil
}

func (p *JobQ) Put(job Job) error {
	return p.inputQ.Put(job)
}

func (p *JobQ) Consume() <-chan error {
	for _, c := range p.consumers {
		go p.process(c)
	}
	return p.errCh
}

func (p *JobQ) process(consumer *Q) {
	for {
		job := p.newJob()
		if err := p.inputQ.PopTo(consumer, &job); err != nil {
			p.sendErr("fail to get job from input queue", job, err)
			continue
		}
		if err := job.Do(); err != nil {
			p.sendErr("fail to do the job", job, err)
			if err := consumer.PopTo(p.failQ, nil); err != nil {
				p.sendErr("fail to put job into the fail queue", job, err)
			}
			continue
		}
		if err := consumer.PopTo(p.doneQ, nil); err != nil {
			p.sendErr("fail to pu job into the done queue", job, err)
		}
	}
}

func (p *JobQ) sendErr(msg string, job Job, err error) {
	p.errCh <- &JobQError{Msg: msg, Job: job, Err: err}
}

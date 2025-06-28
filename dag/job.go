package dag

import 
	(
		"sync"
	)

type TriggerRule string
const (
    AllSuccess TriggerRule = "all_success"
    AllFailed  TriggerRule = "all_failed"
    Always     TriggerRule = "always"
)

type JobStatus string
const (
    JobPending JobStatus = "PENDING"
    JobRunning JobStatus = "RUNNING"
    JobSuccess JobStatus = "SUCCESS"
    JobFailed  JobStatus = "FAILED"
    JobSkipped JobStatus = "SKIPPED"
)

type Job struct {
    ID          string
    action      func(ctx *Context)
    branchFunc  func(ctx *Context) []string
    depends     []*Job
    Upstreams   []*Job
    Downstreams []*Job
    TriggerRule TriggerRule
    status      JobStatus
    statusLock  sync.Mutex
}

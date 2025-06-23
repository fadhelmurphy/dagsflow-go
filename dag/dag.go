package dag

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type Context struct {
	dag *DAG
}

func (c *Context) SetXCom(key string, val interface{}) {
	c.dag.xcomLock.Lock()
	defer c.dag.xcomLock.Unlock()
	c.dag.xcom[key] = val
}

func (c *Context) GetXCom(key string) interface{} {
	c.dag.xcomLock.Lock()
	defer c.dag.xcomLock.Unlock()
	return c.dag.xcom[key]
}

type Job struct {
	ID         string
	Action     func(ctx *Context)
	BranchFunc func(ctx *Context) []string
	Depends    []*Job
	status     string
	statusLock sync.Mutex
}

func (j *Job) DependsOn(parents ...*Job) {
	j.Depends = append(j.Depends, parents...)
}

func (j *Job) Then(next *Job) *Job {
	next.DependsOn(j)
	return next
}

func (j *Job) Branch(nexts ...*Job) {
	for _, next := range nexts {
		next.DependsOn(j)
	}
}

type DAG struct {
	Name     string
	Schedule string
	Jobs     []*Job
	xcom     map[string]interface{}
	xcomLock sync.Mutex
	wg       sync.WaitGroup
	jobMap   map[string]*Job
}

var (
	dagRegistry = make(map[string]*DAG)
	registryLock sync.RWMutex
)

func Register(d *DAG) {
	registryLock.Lock()
	defer registryLock.Unlock()
	dagRegistry[d.Name] = d
}

func Get(name string) (*DAG, bool) {
	registryLock.RLock()
	defer registryLock.RUnlock()
	d, ok := dagRegistry[name]
	return d, ok
}

func ListDAGs() []*DAG {
	registryLock.RLock()
	defer registryLock.RUnlock()
	var list []*DAG
	for _, d := range dagRegistry {
		list = append(list, d)
	}
	return list
}

func NewDAG(name, schedule string) *DAG {
	return &DAG{
		Name:     name,
		Schedule: schedule,
		xcom:     make(map[string]interface{}),
		jobMap:   make(map[string]*Job),
	}
}

func (d *DAG) NewJob(id string, action func(ctx *Context)) *Job {
	job := &Job{ID: id, Action: action, status: "pending"}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) NewBranchJob(id string, branchFunc func(ctx *Context) []string) *Job {
	job := &Job{ID: id, BranchFunc: branchFunc, status: "pending"}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) Run() {
	c := cron.New()
	c.AddFunc(d.Schedule, func() {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.runDAGWithContext(context.Background())
		}()
	})
	c.Start()
	select {} // keep process alive
}

func (d *DAG) RunWithContext(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runDAGWithContext(ctx)
	}()
	d.wg.Wait()
}

func (d *DAG) runDAGWithContext(ctx context.Context) {
	fmt.Printf("Running DAG: %s\n", d.Name)
	myCtx := &Context{dag: d}
	var jobWg sync.WaitGroup

	for _, job := range d.Jobs {
		job.setStatus("pending")
	}

	for _, job := range d.Jobs {
		if len(job.Depends) == 0 {
			jobWg.Add(1)
			go d.runJobWithContext(job, myCtx, &jobWg, ctx)
		}
	}
	jobWg.Wait()
}

func (d *DAG) runJobWithContext(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	defer jobWg.Done()

	for _, dep := range j.Depends {
		for dep.getStatus() != "success" {
			select {
			case <-dagCtx.Done():
				fmt.Printf("Job %s canceled\n", j.ID)
				return
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	j.setStatus("running")
	fmt.Printf("Starting job %s\n", j.ID)

	if j.Action != nil {
		j.Action(ctx)
		j.setStatus("success")
	} else if j.BranchFunc != nil {
		selected := j.BranchFunc(ctx)
		j.setStatus("success")
		for _, selID := range selected {
			if child, ok := d.jobMap[selID]; ok {
				jobWg.Add(1)
				go d.runJobWithContext(child, ctx, jobWg, dagCtx)
			}
		}
	} else {
		j.setStatus("success")
	}

	fmt.Printf("Completed job %s\n", j.ID)

	for _, child := range d.Jobs {
	for _, dep := range child.Depends {
		if dep == j {
			allDepOk := true
			for _, dep := range child.Depends {
				if dep.getStatus() != "success" {
					allDepOk = false
					break
				}
			}
			if allDepOk {
				jobWg.Add(1)
				go d.runJobWithContext(child, ctx, jobWg, dagCtx)
			}
		}
	}
}

}

func (j *Job) getStatus() string {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	return j.status
}

func (j *Job) setStatus(s string) {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	j.status = s
}

func (d *DAG) PrintGraph() {
	childrenMap := make(map[string][]string)
	for _, j := range d.Jobs {
		for _, dep := range j.Depends {
			childrenMap[dep.ID] = append(childrenMap[dep.ID], j.ID)
		}
		if _, exists := childrenMap[j.ID]; !exists {
			childrenMap[j.ID] = []string{}
		}
	}

	fmt.Printf("DAG: %s\n", d.Name)
	for parent, children := range childrenMap {
		if len(children) == 0 {
			fmt.Printf("  %s --> (no children)\n", parent)
		} else {
			fmt.Printf("  %s --> %v\n", parent, children)
		}
	}
}

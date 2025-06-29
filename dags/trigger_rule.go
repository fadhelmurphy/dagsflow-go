package dags

import (
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("trigger_rule_dag", "@every 1s")

	jobA := d.NewJob("job_a", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job A running (success)")
	})

	jobB := d.NewJob("job_b", func(ctx *dag.Context) {
		panic("Simulasi error di Job B")
	})

	jobC := d.NewJob("job_c_all_success", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job C jalan soalnya semua upstream sukses")
	}).WithTriggerRule(dag.AllSuccess)

	jobD := d.NewJob("job_d_all_failed", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job D jalan soalnya semua upstream gagal")
	}).WithTriggerRule(dag.AllFailed)

	jobE := d.NewJob("job_e_always", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job E jalan soalnya trigger rulenya always")
	}).WithTriggerRule(dag.Always)

	jobA.Then(jobC)
	jobB.Then(jobC)

	jobA.Then(jobD)
	jobB.Then(jobD)

	jobA.Then(jobE)
	jobB.Then(jobE)

	dag.Register(d)
}

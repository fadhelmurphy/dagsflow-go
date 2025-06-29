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

	jobAC := d.NewJob("job_a_c_all_success", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job A C jalan soalnya semua upstream sukses")
	}).WithTriggerRule(dag.AllSuccess)

	jobBC := d.NewJob("job_b_all_success", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job B C jalan soalnya semua upstream sukses")
	}).WithTriggerRule(dag.AllSuccess)

	jobAD := d.NewJob("job_a_d_all_failed", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job A D jalan soalnya semua upstream gagal")
	}).WithTriggerRule(dag.AllFailed)

	jobBD := d.NewJob("job_b_d_all_failed", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job B D jalan soalnya semua upstream gagal")
	}).WithTriggerRule(dag.AllFailed)

	jobAE := d.NewJob("job_a_e_always", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job A E jalan soalnya trigger rulenya always")
	}).WithTriggerRule(dag.Always)

	jobBE := d.NewJob("job_b_e_always", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job B E jalan soalnya trigger rulenya always")
	}).WithTriggerRule(dag.Always)

	jobA.Then(jobAC)
	jobB.Then(jobBC)

	jobA.Then(jobAD)
	jobB.Then(jobBD)

	jobA.Then(jobAE)
	jobB.Then(jobBE)

	dag.Register(d)
}

package dags

import (
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("trigger_rule_dag", "@every 1s")

	// Job A - selalu sukses
	jobA := d.NewJob("job_a", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job A running (success)")
	})

	// Job B - gagal (panic)
	jobB := d.NewJob("job_b", func(ctx *dag.Context) {
		panic("Simulasi error di Job B")
	})

	// Job C - jalan jika semua upstream (A dan B) sukses
	jobC := d.NewJob("job_c_all_success", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job C jalan karena semua upstream sukses")
	}).WithTriggerRule(dag.AllSuccess)

	// Job D - jalan jika semua upstream (A dan B) gagal
	jobD := d.NewJob("job_d_all_failed", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job D jalan karena semua upstream gagal")
	}).WithTriggerRule(dag.AllFailed)

	// Job E - jalan walaupun ada yang gagal
	jobE := d.NewJob("job_e_always", func(ctx *dag.Context) {
		ctx.DAG.Logf("Job E jalan karena trigger rule = always")
	}).WithTriggerRule(dag.Always)

	jobA.Then(jobC)
	jobB.Then(jobC)

	jobA.Then(jobD)
	jobB.Then(jobD)

	jobA.Then(jobE)
	jobB.Then(jobE)

	dag.Register(d)
}

package dags

import (
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("custom_dag", "*/1 * * * *")

	start := d.NewJob("start", func(ctx *dag.Context) {
		ctx.DAG.Logf("Start job")
	})

	printA := d.NewJob("print_A", func(ctx *dag.Context) {
		ctx.DAG.Logf("Print A")
	})

	printB := d.NewJob("print_B", func(ctx *dag.Context) {
		ctx.DAG.Logf("Print B")
	})

	printC := d.NewJob("print_C", func(ctx *dag.Context) {
		ctx.DAG.Logf("Print C")
	})

	finish := d.NewJob("finish", func(ctx *dag.Context) {
		ctx.DAG.Logf("Finish job")
	})

	// setup dependency
	start.Branch(printA, printB)
	printA.Then(printC)
	printC.Then(finish)
	printB.Then(finish)

	dag.Register(d)
}

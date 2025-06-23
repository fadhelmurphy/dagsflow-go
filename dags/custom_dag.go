package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("custom_dag", "*/1 * * * *")

	start := d.NewJob("start", func(ctx *dag.Context) {
		fmt.Println("[custom_dag] Start job running")
		ctx.SetXCom("val", 99)
	})

	branch := d.NewBranchJob("branch", func(ctx *dag.Context) []string {
		val := ctx.GetXCom("val").(int)
		if val > 50 {
			fmt.Println("[custom_dag] Pilih print_A")
			return []string{"print_A"}
		}
		fmt.Println("[custom_dag] Pilih print_B")
		return []string{"print_B"}
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
	start.Then(branch)
	branch.Branch(printA, printB)
	printA.Then(printC)
	printC.Then(finish)
	printB.Then(finish)

	dag.Register(d)
}

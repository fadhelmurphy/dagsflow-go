package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag_branch", "@every 1s")

	start := d.NewJob("start", func(ctx *dag.Context) {
		fmt.Println("[dag_branch] Start job running")
		ctx.SetXCom("val", 99)
	})

	branch := d.NewBranchJob("branch", func(ctx *dag.Context) []string {
		val := ctx.GetXCom("val").(int)
		if val > 50 {
			fmt.Println("[dag_branch] Branch chooses high")
			return []string{"high"}
		}
		fmt.Println("[dag_branch] Branch chooses low")
		return []string{"low"}
	})

	high := d.NewJob("high", func(ctx *dag.Context) {
		fmt.Println("[dag_branch] Running HIGH path")
	})

	low := d.NewJob("low", func(ctx *dag.Context) {
		fmt.Println("[dag_branch] Running LOW path")
	})

	// setup dependency
	start.Then(branch)
	branch.Branch(high, low)

	dag.Register(d)
}

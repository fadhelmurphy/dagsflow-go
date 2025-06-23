package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {

	config := map[string]any{
		"threshold": 50,
		"message":   "Hello from config",
	}
	d := dag.NewDAG("custom_dag", "*/1 * * * *", config)

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

	triggerJob := d.NewJob("trigger_dag_branch", func(ctx *dag.Context) {
	config := map[string]interface{}{
		"param1": "value1",
		"param2": 42,
	}

	// ctx.DAG.TriggerDAG("dag_branch") // tanpa config
	ctx.DAG.TriggerDAGWithConfig("dag_branch",config, false) // dengan config 
	}) // Non Blocking example (tidak perlu nunggu dag nya kelar)

	triggerBlockingJob := d.NewJob("trigger_dag1", func(ctx *dag.Context) {
			config := map[string]any{
		"param1": "value1",
		"param2": 42,
	}
	// ctx.DAG.TriggerDAGBlocking("dag1") // tanpa config
	ctx.DAG.TriggerDAGWithConfig("dag1", config, true) // dengan config 
	}) // Blocking example (perlu nunggu dag nya kelar)


	// setup dependency
	start.Then(branch)
	branch.Branch(printA, printB)
	printA.Then(printC)
	printC.Then(finish)
	printB.Then(finish)
	finish.Then(triggerBlockingJob).Then(triggerJob)

	dag.Register(d)
}

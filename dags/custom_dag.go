package dags

import (
	"dagsflow-go/dag"
	"fmt"
)

func init() {

	config := map[string]any{
		"threshold": 99,
		"message":   "Hello from config",
	}
	d := dag.NewDAG("custom_dag", "*/1 * * * *", config)

	start := d.NewJob("start", func(ctx *dag.Context) {
		val := ctx.DAG.Config["threshold"]
		fmt.Println("[custom_dag] Start job running")
		ctx.SetXCom("val", val)
	})

	branch := d.NewBranchJob("branch", func(ctx *dag.Context) []string {
		val := ctx.GetXCom("val").(int)
		fmt.Printf("%d << get xcom\n", val)
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
		config := map[string]any{
			"param1": "value1",
			"param2": 42,
		}

		// ctx.DAG.TriggerDAG("dag_branch") // tanpa config
		ctx.DAG.TriggerDAGWithConfig("dag_branch", config, false) // dengan config
	}) // Non Blocking (tidak perlu nunggu triggered dag nya kelar)

	triggerBlockingJob := d.NewJob("trigger_dag1", func(ctx *dag.Context) {
		config := map[string]any{
			"param1": "value1",
			"param2": 42,
		}
		ctx.DAG.TriggerDAGWithConfig("dag1", config, true)
	})

	bqJob := d.NewBigQueryJob("bq_task_1", "dags/dw_to_tmp.sql", map[string]any{
		"project_id":     "idf-corp-dev",
		"target_dataset": "tmp_ds",
		"target_table":   "tmp_table",
	})

	// setup dependency
	start.Then(branch)
	branch.Branch(printA, printB)
	printA.Then(printC)
	printC.Then(finish)
	printB.Then(finish)
	finish.Then(bqJob).Then(triggerBlockingJob).Then(triggerJob)

	dag.Register(d)
}

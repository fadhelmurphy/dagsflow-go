package dags

import (
	"dagsflow-go/dag"
	"fmt"
)

func init() {

	config := map[string]any{
		"threshold":  99,
		"message":    "Hello from config",
		"project_id": "fadhel-project",
	}
	d := dag.NewDAG("custom_dag", "@every 1s", config)

	start := d.NewJob("start", func(ctx *dag.Context) {
		val := ctx.DAG.Config["threshold"]
		project_id := ctx.DAG.Config["project_id"]
		fmt.Println("[custom_dag] Start job running")
		ctx.SetXCom("val", val)
		ctx.SetXCom("project_id", project_id)
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

	finishA := d.NewJob("finishA", func(ctx *dag.Context) {
		ctx.DAG.Logf("Finish job A")
	})

	finishB := d.NewJob("finishB", func(ctx *dag.Context) {
		ctx.DAG.Logf("Finish job B")
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

	bqJob := d.NewBigQueryJob("bq_task_1", "dags/sql/dw_to_tmp.sql", map[string]any{
		"project_id":     `{{xcom "project_id"}}`,
		"target_dataset": "dw_ext_data",
		"target_table":   "kemdikbud_ref_satpen_npsn",
	})

	// setup dependency
	start.Then(branch)
	branch.Branch(printA, printB)
	printA.Then(printC)
	printC.Then(finishA)
	printB.Then(finishB)
	finishA.Then(bqJob).Then(triggerBlockingJob).Then(triggerJob)

	dag.Register(d)
}

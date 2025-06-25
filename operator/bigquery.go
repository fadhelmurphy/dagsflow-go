package operator

import "fmt"

type BigQueryOperator struct {
	TaskID    string
	Query     string
	ProjectID string
	Logf      func(format string, args ...interface{})
	LogErrorf func(format string, args ...interface{})
}

func (op *BigQueryOperator) Execute() {
	if op.ProjectID == "" {
		op.LogErrorf("[BigQueryOperator %s] Project ID is empty", op.TaskID)
		return
	}
	if op.Query == "" {
		op.LogErrorf("[BigQueryOperator %s] Query is empty", op.TaskID)
		return
	}

	op.Logf("[BigQueryOperator %s] Running query on project %s", op.TaskID, op.ProjectID)
	op.Logf("[BigQueryOperator %s] Query:\n%s", op.TaskID, op.Query)

	fmt.Printf("Simulated run of BigQuery query for project: %s\n", op.ProjectID)
}

package dag

import (
	"bytes"
	"dagsflow-go/operator"
	"os"
	"text/template"
)

func (d *DAG) NewBigQueryJob(id string, queryPath string, params map[string]any) *Job {
	op := &operator.BigQueryOperator{
		TaskID:    id,
		Logf:      d.Logf,
		LogErrorf: d.LogErrorf,
	}

	job := d.NewJob(id, func(ctx *Context) {
		// Baca file query template
		raw, err := os.ReadFile(queryPath)
		if err != nil {
			d.LogErrorf("Failed to read query file %s: %v", queryPath, err)
			return
		}

		// Render template
		tmpl, err := template.New("bq_query").Parse(string(raw))
		if err != nil {
			d.LogErrorf("Failed to parse query template: %v", err)
			return
		}

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, params); err != nil {
			d.LogErrorf("Failed to render query template: %v", err)
			return
		}
		op.Query = buf.String()

		// Resolve projectID
		projectID := ""
		if val, ok := params["project_id"].(string); ok && val != "" {
			projectID = val
		} else if val, ok := d.Config["project_id"].(string); ok && val != "" {
			projectID = val
		} else if conn, ok := d.Connections["env_bigquery"]; ok {
			if val, ok := conn.Config["project_id"].(string); ok && val != "" {
				projectID = val
			}
		}

		op.ProjectID = projectID
		op.Execute()
	})

	return job
}

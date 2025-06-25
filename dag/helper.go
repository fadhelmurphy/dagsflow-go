package dag

import (
	"bytes"
	"dagsflow-go/operator"
	"os"
	"text/template"
)

func RenderStringTemplate(raw string, params map[string]any) (string, error) {
	tmpl, err := template.New("param").Parse(raw)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, params)
	return buf.String(), err
}

func (d *DAG) NewBigQueryJob(id string, queryPath string, params map[string]any) *Job {
	op := &operator.BigQueryOperator{
		TaskID:    id,
		Logf:      d.Logf,
		LogErrorf: d.LogErrorf,
	}

	job := d.NewJob(id, func(ctx *Context) {
		raw, err := os.ReadFile(queryPath)
		if err != nil {
			d.LogErrorf("Failed to read query file %s: %v", queryPath, err)
			return
		}

		funcMap := template.FuncMap{
			"xcom": func(key string) string {
				val := ctx.GetXCom(key)
				if str, ok := val.(string); ok {
					return str
				}
				return ""
			},
		}

		// Parse & render template SQL
		tmpl, err := template.New("bq_query").Funcs(funcMap).Parse(string(raw))
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

		// Render project_id
		projectID := ""
		if val, ok := params["project_id"].(string); ok && val != "" {
			var bufID bytes.Buffer
			tmplID, _ := template.New("project_id").Funcs(funcMap).Parse(val)
			_ = tmplID.Execute(&bufID, params)
			projectID = bufID.String()
		} else if val, ok := ctx.GetXCom("project_id").(string); ok {
			projectID = val
		} else if val, ok := d.Config["project_id"].(string); ok {
			projectID = val
		} else if conn, ok := d.Connections["env_bigquery"]; ok {
			if val, ok := conn.Config["project_id"].(string); ok {
				projectID = val
			}
		}

		op.ProjectID = projectID
		op.Execute()
	})

	return job
}

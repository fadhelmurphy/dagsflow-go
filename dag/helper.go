package dag

import (
	"bytes"
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

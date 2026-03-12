package gaussdb

import "strings"

const (
	statusOK   = "OK"
	statusNG   = "NG"
	statusNone = "NONE"
)

type parsedOutput struct {
	Name    string
	Status  string
	Summary string
	Raw     string
}

func parseOutput(content string) parsedOutput {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	section := ""
	summary := make([]string, 0, len(lines))
	raw := make([]string, 0, len(lines))
	output := parsedOutput{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.Contains(line, "[NAM]"):
			section = "name"
			output.Name = strings.TrimSpace(afterToken(line, "[NAM]"))
		case strings.Contains(line, "[RST]"):
			section = "status"
			output.Status = strings.TrimSpace(afterToken(line, "[RST]"))
		case strings.Contains(line, "[RAW]"):
			section = "raw"
		case strings.Contains(line, "[STD]"):
			section = "summary"
		case section == "summary" || section == "status":
			if trimmed != "" {
				summary = append(summary, trimmed)
			}
		case section == "raw":
			raw = append(raw, line)
		}
	}
	output.Summary = strings.TrimSpace(strings.Join(summary, "\n"))
	output.Raw = strings.TrimSpace(strings.Join(raw, "\n"))
	return output
}

func afterToken(line string, token string) string {
	index := strings.Index(line, token)
	if index < 0 {
		return line
	}
	return line[index+len(token):]
}

func normalizeStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case statusOK:
		return "normal"
	case statusNG:
		return "abnormal"
	case statusNone:
		return "not_applicable"
	default:
		return "unknown"
	}
}

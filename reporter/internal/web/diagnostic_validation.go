package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/launcher"
	"dbcheck/reporter/internal/users"
)

// DiagnosticCheck records only the correspondence the enrichment checks prove.
type DiagnosticCheck struct {
	ItemPosition       int    `json:"itemPosition"`
	AttachmentPosition int    `json:"attachmentPosition"`
	FileName           string `json:"fileName"`
	Kind               string `json:"kind"`
	Message            string `json:"message,omitempty"`
	Evidence           string `json:"evidence,omitempty"`
}

type diagnosticValidator interface {
	ValidateDiagnostics(context.Context, string, ItemInput) ([]DiagnosticCheck, error)
}

func (h *apiHandler) handleValidateDiagnostics(w http.ResponseWriter, r *http.Request, _ users.User) {
	if h.cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	}
	if err := r.ParseMultipartForm(multipartMemoryBytes); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeAPIError(w, errUploadTooLarge)
		} else {
			h.writeAPIError(w, apierr.Invalid("请至少上传一个 ZIP 文件"))
		}
		return
	}
	defer r.MultipartForm.RemoveAll()
	zips := filesForKey(r.MultipartForm, "zips")
	if len(zips) == 0 {
		zips = filesForKey(r.MultipartForm, "zip")
	}
	if len(zips) == 0 {
		h.writeAPIError(w, apierr.Invalid("请至少上传一个 ZIP 文件"))
		return
	}
	dir, err := os.MkdirTemp(h.cfg.DataDir, "diagnostic-validation-")
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	defer os.RemoveAll(dir)
	form := uploadForm{form: r.MultipartForm, zips: zips}
	items, err := form.stage(filepath.Join(dir, "uploads"))
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	validator, ok := h.platform.Pipeline.(diagnosticValidator)
	if !ok {
		executable, err := os.Executable()
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		validator = NewPipeline(executable, h.cfg.PythonBin)
	}
	checks := []DiagnosticCheck{}
	for i, item := range items {
		id := strconv.Itoa(i + 1)
		input := ItemInput{ID: id, Name: item.FileName, ZipPath: filepath.Join(dir, "uploads", "zip-"+id+"-"+item.FileName)}
		names := []string{}
		results := []DiagnosticCheck{}
		validPositions := []int{}
		addAttachment := func(kind, name, path string) {
			names = append(names, name)
			candidate := ItemInput{}
			if kind == "awr" {
				candidate.AWRPath = path
			} else {
				candidate.WDRPaths = []string{path}
			}
			if typeErr := validateHTMLAttachment(item.DBType, candidate); typeErr != nil {
				results = append(results, DiagnosticCheck{Kind: "invalid", Message: "附件类型与采集包不符，请移除并使用对应的 AWR/WDR 报告。" + typeErr.Error()})
				return
			}
			validPositions = append(validPositions, len(results))
			results = append(results, DiagnosticCheck{})
			if kind == "awr" {
				input.AWRPath = path
			} else {
				input.WDRPaths = append(input.WDRPaths, path)
			}
		}
		for _, hdr := range form.paired("awr", i) {
			name := filepath.Base(hdr.Filename)
			addAttachment("awr", name, filepath.Join(dir, "uploads", "awr-"+id+"-"+name))
		}
		for k, hdr := range form.paired("wdr", i) {
			name := filepath.Base(hdr.Filename)
			addAttachment("wdr", name, filepath.Join(dir, "uploads", "wdr-"+wdrUploadID(id, k)+"-"+name))
		}
		if len(names) == 0 {
			continue
		}
		if len(validPositions) > 0 {
			validated, err := validator.ValidateDiagnostics(r.Context(), dir, input)
			if err != nil {
				h.writeAPIError(w, err)
				return
			}
			if len(validated) != len(validPositions) {
				h.writeAPIError(w, fmt.Errorf("diagnostic validation returned an incomplete result"))
				return
			}
			for k, check := range validated {
				results[validPositions[k]] = check
			}
		}
		for k, result := range results {
			result.ItemPosition = i + 1
			result.AttachmentPosition = k + 1
			result.FileName = names[k]
			checks = append(checks, result)
		}
	}
	writeJSON(w, http.StatusOK, checks)
}

func (p *Pipeline) ValidateDiagnostics(ctx context.Context, dir string, input ItemInput) ([]DiagnosticCheck, error) {
	extract := filepath.Join(dir, "items", input.ID, "extract")
	if err := os.MkdirAll(extract, 0o755); err != nil {
		return nil, err
	}
	if err := p.ExtractZip(input.ZipPath, extract); err != nil {
		return nil, err
	}
	runDir, err := p.DetectRun(extract)
	if err != nil {
		return nil, err
	}
	cfg := launcher.Config{RunDir: runDir, AWRFile: input.AWRPath, WDRFiles: input.WDRPaths}
	layout, err := p.LayoutResolver.Resolve(p.ExecutablePath, cfg)
	if err != nil {
		return nil, err
	}
	args := append([]string{layout.Script}, launcher.OrchestratorArgs(cfg, layout)...)
	args = append(args, "--validate-diagnostics")
	output, err := exec.CommandContext(ctx, p.PythonBin, args...).Output()
	if err != nil {
		return nil, fmt.Errorf("附件校验服务失败，请重试：%w", err)
	}
	var checks []DiagnosticCheck
	if err := json.Unmarshal(output, &checks); err != nil {
		return nil, fmt.Errorf("invalid diagnostic validation response: %w", err)
	}
	for _, check := range checks {
		if check.Kind != "checked" && check.Kind != "invalid" {
			return nil, fmt.Errorf("unknown diagnostic validation result: %q", check.Kind)
		}
		if check.Kind == "invalid" && strings.TrimSpace(check.Message) == "" {
			return nil, fmt.Errorf("missing diagnostic failure detail")
		}
	}
	return checks, nil
}

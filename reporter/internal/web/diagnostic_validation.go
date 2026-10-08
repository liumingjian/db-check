package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/launcher"
	"dbcheck/reporter/internal/reports"
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
	form, err := h.diagnosticUploadForm(w, r)
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	defer r.MultipartForm.RemoveAll()
	dir, err := os.MkdirTemp(h.cfg.DataDir, "diagnostic-validation-")
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	defer os.RemoveAll(dir)
	items, err := form.stage(filepath.Join(dir, "uploads"))
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	validator, err := h.diagnosticValidator()
	if err != nil {
		h.writeAPIError(w, err)
		return
	}
	checks := []DiagnosticCheck{}
	for i, item := range items {
		results, err := validateDiagnosticItem(r.Context(), validator, dir, form, i, item)
		if err != nil {
			h.writeAPIError(w, err)
			return
		}
		checks = append(checks, results...)
	}
	writeJSON(w, http.StatusOK, checks)
}

func (h *apiHandler) diagnosticUploadForm(w http.ResponseWriter, r *http.Request) (uploadForm, error) {
	if h.cfg.MaxUploadBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	}
	if err := r.ParseMultipartForm(multipartMemoryBytes); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return uploadForm{}, errUploadTooLarge
		}
		return uploadForm{}, apierr.Invalid("请至少上传一个 ZIP 文件")
	}
	zips := filesForKey(r.MultipartForm, "zips")
	if len(zips) == 0 {
		zips = filesForKey(r.MultipartForm, "zip")
	}
	if len(zips) == 0 {
		r.MultipartForm.RemoveAll()
		return uploadForm{}, apierr.Invalid("请至少上传一个 ZIP 文件")
	}
	return uploadForm{form: r.MultipartForm, zips: zips}, nil
}

func (h *apiHandler) diagnosticValidator() (diagnosticValidator, error) {
	if validator, ok := h.platform.Pipeline.(diagnosticValidator); ok {
		return validator, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return NewPipeline(executable, h.cfg.PythonBin), nil
}

type diagnosticAttachment struct{ kind, name, path string }

func (f uploadForm) diagnosticAttachments(dir string, position int) []diagnosticAttachment {
	id := strconv.Itoa(position + 1)
	attachments := []diagnosticAttachment{}
	for _, kind := range htmlInputKinds {
		for k, header := range f.paired(kind, position) {
			uploadID := id
			if kind == "wdr" {
				uploadID = wdrUploadID(id, k)
			}
			name := filepath.Base(header.Filename)
			attachments = append(attachments, diagnosticAttachment{kind, name, uploadPath(filepath.Join(dir, "uploads"), kind, uploadID, name)})
		}
	}
	return attachments
}

func prepareDiagnosticInput(dir string, item reports.Item, attachments []diagnosticAttachment) (ItemInput, []DiagnosticCheck, []int) {
	id := strconv.Itoa(item.Position)
	input := ItemInput{ID: id, Name: item.FileName, ZipPath: uploadPath(filepath.Join(dir, "uploads"), "zip", id, item.FileName)}
	results := make([]DiagnosticCheck, len(attachments))
	validPositions := []int{}
	for k, attachment := range attachments {
		candidate := ItemInput{}
		if attachment.kind == "awr" {
			candidate.AWRPath = attachment.path
		} else {
			candidate.WDRPaths = []string{attachment.path}
		}
		if err := validateHTMLAttachment(item.DBType, candidate); err != nil {
			results[k] = DiagnosticCheck{Kind: "invalid", Message: "附件类型与采集包不符，请移除并使用对应的 AWR/WDR 报告。" + err.Error()}
			continue
		}
		validPositions = append(validPositions, k)
		if attachment.kind == "awr" {
			input.AWRPath = attachment.path
		} else {
			input.WDRPaths = append(input.WDRPaths, attachment.path)
		}
	}
	return input, results, validPositions
}

func validateDiagnosticItem(ctx context.Context, validator diagnosticValidator, dir string, form uploadForm, position int, item reports.Item) ([]DiagnosticCheck, error) {
	attachments := form.diagnosticAttachments(dir, position)
	input, results, validPositions := prepareDiagnosticInput(dir, item, attachments)
	if len(validPositions) > 0 {
		validated, err := validator.ValidateDiagnostics(ctx, dir, input)
		if err != nil {
			return nil, err
		}
		if len(validated) != len(validPositions) {
			return nil, fmt.Errorf("diagnostic validation returned an incomplete result")
		}
		for k, check := range validated {
			results[validPositions[k]] = check
		}
	}
	for k := range results {
		results[k].ItemPosition = position + 1
		results[k].AttachmentPosition = k + 1
		results[k].FileName = attachments[k].name
	}
	return results, nil
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
	output, err := p.Runner.Output(ctx, p.PythonBin, args)
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

// Startup precedes request admission in the single API process (ADR 0003).
// Every validation directory left by the previous process is abandoned.
func removeAbandonedDiagnosticValidations(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "diagnostic-validation-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dataDir, entry.Name())); err != nil {
			return fmt.Errorf("remove abandoned diagnostic validation: %w", err)
		}
	}
	return nil
}

package web

import (
	"fmt"
	"mime/multipart"
	"os"
	"strconv"
	"strings"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/reports"
)

// htmlInputKinds are the optional HTML inputs paired with a ZIP: an Oracle
// AWR file or GaussDB WDR files. Each comes either as "<kind>s", one file per ZIP
// in order, or as "<kind>_<n>" for the n-th ZIP.
var htmlInputKinds = []string{"awr", "wdr"}

// uploadForm is a report submission: its multipart form and its ZIPs.
type uploadForm struct {
	form *multipart.Form
	zips []*multipart.FileHeader
}

// stage saves every uploaded file under uploadsDir, named as loadTaskInputs
// reads them back, and returns the report items with what the server read
// from each ZIP.
func (f uploadForm) stage(uploadsDir string) ([]reports.Item, error) {
	for _, kind := range htmlInputKinds {
		if all := filesForKey(f.form, kind+"s"); len(all) != 0 && len(all) != len(f.zips) {
			upper := strings.ToUpper(kind)
			return nil, apierr.Invalid(fmt.Sprintf("%s 文件数量与 ZIP 不一致，请按序号上传 %s_<n>", upper, kind))
		}
	}
	if err := os.MkdirAll(uploadsDir, 0o755); err != nil {
		return nil, err
	}
	items := make([]reports.Item, 0, len(f.zips))
	for i := range f.zips {
		item, err := f.stageItem(uploadsDir, i)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// stageItem saves ZIP i and its paired files, and reads the ZIP.
func (f uploadForm) stageItem(uploadsDir string, i int) (reports.Item, error) {
	itemID := strconv.Itoa(i + 1)
	zipPath, name, err := saveUpload(uploadsDir, "zip", itemID, f.zips[i])
	if err != nil {
		return reports.Item{}, err
	}
	read, err := reports.ReadCollectorZip(zipPath)
	if err != nil {
		return reports.Item{}, apierr.Invalid(name + "：" + err.Error())
	}
	awrs := f.paired("awr", i)
	if len(awrs) > 1 {
		return reports.Item{}, apierr.Invalid(name + "：每个 ZIP 只能搭配一个 Oracle AWR 文件")
	}
	for _, hdr := range awrs {
		if _, _, err := saveUpload(uploadsDir, "awr", itemID, hdr); err != nil {
			return reports.Item{}, err
		}
	}
	for k, hdr := range f.paired("wdr", i) {
		if _, _, err := saveUpload(uploadsDir, "wdr", wdrUploadID(itemID, k), hdr); err != nil {
			return reports.Item{}, err
		}
	}
	return reports.Item{
		Position: i + 1, FileName: name, DBType: read.DBType,
		CollectorVersion: read.CollectorVersion, Status: reports.StatusQueued,
	}, nil
}

// paired returns the files of kind (see htmlInputKinds) that go with ZIP i.
func (f uploadForm) paired(kind string, i int) []*multipart.FileHeader {
	if all := filesForKey(f.form, kind+"s"); len(all) == len(f.zips) {
		return all[i : i+1]
	}
	return filesForKey(f.form, kind+"_"+strconv.Itoa(i+1))
}

func wdrUploadID(itemID string, existing int) string {
	return fmt.Sprintf("%s-%d", itemID, existing+1)
}

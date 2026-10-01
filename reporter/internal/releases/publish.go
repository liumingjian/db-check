package releases

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"dbcheck/reporter/internal/apierr"
	"dbcheck/reporter/internal/store"
)

// Metadata is the publish request's metadata part (docs/openapi).
type Metadata struct {
	Version string `json:"version"`
	Tag     string `json:"tag"`
	Commit  string `json:"commit"`
	// PublishedAt is an RFC 3339 time.
	PublishedAt string            `json:"publishedAt"`
	Notes       string            `json:"notes"`
	DBTypes     []string          `json:"dbTypes"`
	Packages    []PackageMetadata `json:"packages"`
}

// PackageMetadata is what the publisher claims about one package file.
type PackageMetadata struct {
	Platform Platform `json:"platform"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
}

// DBTypes are the database types a release may support.
var DBTypes = []string{"mysql", "oracle", "gaussdb"}

var (
	finalVersion = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
	rcVersion    = regexp.MustCompile(`^\d+\.\d+\.\d+-rc\d+$`)
)

// Upload holds a publish request's package files, staged in a hidden
// directory of the releases area until Publish moves them into place, so a
// refused or half-received publish never leaves a package file behind.
type Upload struct {
	dataDir string
	dir     string
	files   map[Platform]stagedFile
}

type stagedFile struct {
	size   int64
	sha256 string
}

// NewUpload starts staging one publish request's files. Always Discard it.
func NewUpload(dataDir string) (*Upload, error) {
	if err := os.MkdirAll(Dir(dataDir), 0o755); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(Dir(dataDir), ".upload-")
	if err != nil {
		return nil, err
	}
	return &Upload{dataDir: dataDir, dir: dir, files: map[Platform]stagedFile{}}, nil
}

// Discard removes whatever Publish did not move into place.
func (u *Upload) Discard() {
	os.RemoveAll(u.dir)
}

// Add stages the package file for platform, recording its size and SHA256.
// It refuses an unknown or repeated platform, and a file that is not a zip.
// Read errors are returned wrapped, so a caller can recognise its own body
// limit.
func (u *Upload) Add(platform Platform, fileName string, r io.Reader) error {
	if !slices.Contains(Platforms, platform) {
		return apierr.Invalid(fmt.Sprintf("不支持的平台 %s，只接受 %v", platform, Platforms))
	}
	if _, dup := u.files[platform]; dup {
		return apierr.Invalid(fmt.Sprintf("%s 安装包重复", platform))
	}
	if !strings.HasSuffix(strings.ToLower(fileName), ".zip") {
		return apierr.Invalid(fmt.Sprintf("%s 安装包必须是 .zip 文件，收到 %s", platform, fileName))
	}
	path := u.stagedPath(platform)
	staged, err := writeHashed(path, r)
	if err != nil {
		return fmt.Errorf("stage %s package: %w", platform, err)
	}
	zr, err := zip.OpenReader(path)
	if err != nil {
		return apierr.Invalid(fmt.Sprintf("%s 安装包不是有效的 zip 文件", platform))
	}
	zr.Close()
	u.files[platform] = staged
	return nil
}

func (u *Upload) stagedPath(p Platform) string {
	return filepath.Join(u.dir, string(p)+".zip")
}

func writeHashed(path string, r io.Reader) (stagedFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return stagedFile{}, err
	}
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, h), r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return stagedFile{size: size, sha256: hex.EncodeToString(h.Sum(nil))}, err
}

// Publish verifies the staged files against meta and stores the release: a
// vX.Y.Z tag publishes as latest, demoting the previous latest; a
// vX.Y.Z-rcN tag as a pre-release. The package files move into place inside
// the insert's transaction, so a release is visible only once complete.
//
// Re-publishing a version with the same tag, commit, and package files
// returns the stored release with created false and changes nothing;
// anything else for a published version is a conflict.
func Publish(ctx context.Context, db *store.DB, u *Upload, meta Metadata) (r Release, created bool, err error) {
	r, err = u.verify(meta)
	if err != nil {
		return Release{}, false, err
	}
	err = db.Tx(ctx, func(tx store.Querier) error {
		existing, found, err := find(ctx, tx, r.Version)
		if err != nil {
			return err
		}
		if found {
			if !sameContent(existing, r) {
				return apierr.Conflict(fmt.Sprintf("版本 %s 已发布，且内容与本次不同", r.Version))
			}
			r = existing
			return nil
		}
		if err := Insert(ctx, tx, r); err != nil {
			return err
		}
		created = true
		return u.moveInto(r.Version)
	})
	if err != nil {
		if created {
			os.RemoveAll(filepath.Join(Dir(u.dataDir), r.Version))
		}
		return Release{}, false, err
	}
	return r, created, nil
}

// verify checks meta and the staged files against each other and returns
// the release to store.
func (u *Upload) verify(meta Metadata) (Release, error) {
	v := meta.Version
	var status Status
	switch {
	case finalVersion.MatchString(v):
		status = StatusLatest
	case rcVersion.MatchString(v):
		status = StatusPreRelease
	default:
		return Release{}, apierr.Invalid(fmt.Sprintf("版本号 %q 必须是 X.Y.Z 或 X.Y.Z-rcN", v))
	}
	if meta.Tag != "v"+v {
		return Release{}, apierr.Invalid(fmt.Sprintf("标签 %q 与版本 %s 不一致，应为 v%s", meta.Tag, v, v))
	}
	if strings.TrimSpace(meta.Commit) == "" {
		return Release{}, apierr.Invalid("缺少提交 (commit)")
	}
	publishedAt, err := time.Parse(time.RFC3339, meta.PublishedAt)
	if err != nil {
		return Release{}, apierr.Invalid(fmt.Sprintf("发布时间 %q 不是 RFC 3339 时间", meta.PublishedAt))
	}
	if len(meta.DBTypes) == 0 {
		return Release{}, apierr.Invalid("缺少支持的数据库类型")
	}
	for _, t := range meta.DBTypes {
		if !slices.Contains(DBTypes, t) {
			return Release{}, apierr.Invalid(fmt.Sprintf("不支持的数据库类型 %s", t))
		}
	}
	pkgs, err := u.verifyPackages(v, meta.Packages)
	if err != nil {
		return Release{}, err
	}
	return Release{
		Version: v, Tag: meta.Tag, Commit: strings.TrimSpace(meta.Commit),
		PublishedAt: store.FormatTime(publishedAt), Status: status,
		Notes: meta.Notes, DBTypes: meta.DBTypes, Packages: pkgs,
	}, nil
}

func (u *Upload) verifyPackages(version string, claimed []PackageMetadata) ([]Package, error) {
	byPlatform := map[Platform]PackageMetadata{}
	for _, p := range claimed {
		if !slices.Contains(Platforms, p.Platform) {
			return nil, apierr.Invalid(fmt.Sprintf("不支持的平台 %s，只接受 %v", p.Platform, Platforms))
		}
		if _, dup := byPlatform[p.Platform]; dup {
			return nil, apierr.Invalid(fmt.Sprintf("%s 安装包重复", p.Platform))
		}
		byPlatform[p.Platform] = p
	}
	out := make([]Package, 0, len(Platforms))
	for _, platform := range Platforms {
		claim, ok := byPlatform[platform]
		if !ok {
			return nil, apierr.Invalid(fmt.Sprintf("缺少 %s 安装包的元数据", platform))
		}
		file, ok := u.files[platform]
		if !ok {
			return nil, apierr.Invalid(fmt.Sprintf("缺少 %s 安装包文件", platform))
		}
		if file.size != claim.Size {
			return nil, apierr.Invalid(fmt.Sprintf("%s 安装包大小不符：声明 %d 字节，实际 %d 字节", platform, claim.Size, file.size))
		}
		if file.sha256 != strings.ToLower(claim.SHA256) {
			return nil, apierr.Invalid(fmt.Sprintf("%s 安装包 SHA256 不符：声明 %s，实际 %s", platform, claim.SHA256, file.sha256))
		}
		out = append(out, newPackage(version, platform, file.size, file.sha256))
	}
	return out, nil
}

func sameContent(a, b Release) bool {
	return a.Tag == b.Tag && a.Commit == b.Commit && slices.Equal(a.Packages, b.Packages)
}

// moveInto puts the staged files at their PackagePath. A directory left by
// an earlier publish that never committed is replaced.
func (u *Upload) moveInto(version string) error {
	target := filepath.Join(Dir(u.dataDir), version)
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	if err := os.Mkdir(target, 0o755); err != nil {
		return err
	}
	for _, p := range Platforms {
		if err := os.Rename(u.stagedPath(p), PackagePath(u.dataDir, version, p)); err != nil {
			return err
		}
	}
	return nil
}

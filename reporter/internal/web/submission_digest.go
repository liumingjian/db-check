package web

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
)

const submissionDigestVersion = "db-check-report-submission-v1"

// submissionDigest uses length-framed fields so the input structure cannot be
// confused by filenames or contents that contain delimiter bytes.
type submissionDigest struct {
	hash hash.Hash
}

func newSubmissionDigest() *submissionDigest {
	digest := &submissionDigest{hash: sha256.New()}
	digest.writeString(submissionDigestVersion)
	return digest
}

func (d *submissionDigest) writeString(value string) {
	d.writeBytes([]byte(value))
}

func (d *submissionDigest) writeBytes(value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = d.hash.Write(length[:])
	_, _ = d.hash.Write(value)
}

func (d *submissionDigest) writeNumber(value int) {
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(value))
	_, _ = d.hash.Write(number[:])
}

func (d *submissionDigest) writeFile(role, originalName string, contentDigest []byte) {
	d.writeString(role)
	d.writeString(originalName)
	d.writeBytes(contentDigest)
}

func (d *submissionDigest) sum() string {
	return hex.EncodeToString(d.hash.Sum(nil))
}

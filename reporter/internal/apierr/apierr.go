// Package apierr is the error envelope every platform API route answers with:
// HTTP status plus `{"code": "<ApiError code>", "message": "<中文提示>"}`.
// The codes are the console contract's ApiErrorCode values
// (web/src/lib/api/errors.ts); the console shows message as-is, so it must be
// a Chinese user-facing sentence.
//
// Domain packages return these errors for rule violations; the web package
// writes them (web.writeAPIError). Any other error becomes `failed`.
package apierr

import "net/http"

type Code string

const (
	CodeUnauthorized Code = "unauthorized"
	CodeForbidden    Code = "forbidden"
	CodeNotFound     Code = "not_found"
	CodeInvalid      Code = "invalid"
	CodeFailed       Code = "failed"
)

// Error is an API error with the HTTP status it is answered with.
type Error struct {
	Status  int    `json:"-"`
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// Unauthorized (401): no session, or the session is gone.
func Unauthorized(message string) *Error {
	return &Error{http.StatusUnauthorized, CodeUnauthorized, message}
}

// Forbidden (403): signed in, but the role or account status does not allow it.
func Forbidden(message string) *Error {
	return &Error{http.StatusForbidden, CodeForbidden, message}
}

// NotFound (404): no such record, or one the caller may not see.
func NotFound(message string) *Error {
	return &Error{http.StatusNotFound, CodeNotFound, message}
}

// Invalid (400): the request breaks a rule.
func Invalid(message string) *Error {
	return &Error{http.StatusBadRequest, CodeInvalid, message}
}

// Conflict (409, code invalid): the request clashes with existing data.
func Conflict(message string) *Error {
	return &Error{http.StatusConflict, CodeInvalid, message}
}

// TooLarge (413, code invalid): the upload exceeds the configured limit.
func TooLarge(message string) *Error {
	return &Error{http.StatusRequestEntityTooLarge, CodeInvalid, message}
}

// Failed (500): anything else.
func Failed(message string) *Error {
	return &Error{http.StatusInternalServerError, CodeFailed, message}
}

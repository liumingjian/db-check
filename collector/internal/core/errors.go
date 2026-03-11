package core

import "fmt"

const (
	ExitSuccess        = 0
	ExitPartial        = 10
	ExitCollectFailed  = 20
	ExitPrecheckFailed = 30
)

type PrecheckError struct {
	Message string
}

func (e PrecheckError) Error() string {
	return e.Message
}

type CollectionError struct {
	Message string
}

func (e CollectionError) Error() string {
	return e.Message
}

type RunnerError struct {
	ExitCode int
	Err      error
}

func (e RunnerError) Error() string {
	return fmt.Sprintf("exit=%d: %v", e.ExitCode, e.Err)
}

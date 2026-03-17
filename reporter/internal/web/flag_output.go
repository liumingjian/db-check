package web

import "io"

func discardFlagOutput() io.Writer {
	return io.Discard
}

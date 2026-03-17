package web

import "time"

func shutdownTimeout() time.Duration {
	return 10 * time.Second
}

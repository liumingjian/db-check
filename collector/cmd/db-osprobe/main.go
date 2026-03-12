package main

import (
	"dbcheck/collector/internal/osinfo"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	os.Exit(run())
}

func run() int {
	payload, err := osinfo.CollectSinglePayload()
	if err != nil {
		fmt.Fprintf(os.Stderr, "os probe failed: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		fmt.Fprintf(os.Stderr, "encode os probe payload failed: %v\n", err)
		return 1
	}
	return 0
}

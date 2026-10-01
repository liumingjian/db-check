// Command db-web-testserver runs the contract test server (package
// testserver) for the console's "real" contract entry. web/'s Vitest global
// setup builds and starts it; it is never built into production.
//
// It prints "listening on http://<addr>" once it accepts connections.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"dbcheck/reporter/internal/testserver"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "listen address; port 0 picks a free port")
	seed := flag.String("seed", "tests/fixtures/console-seed.json", "the shared seed fixture")
	flag.Parse()

	if err := serve(*addr, *seed); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		os.Exit(1)
	}
}

func serve(addr, seed string) error {
	dataDir, err := os.MkdirTemp("", "db-web-testserver-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dataDir)
	s, err := testserver.New(dataDir, seed)
	if err != nil {
		return err
	}
	defer s.Close()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("listening on http://%s\n", ln.Addr())
	return http.Serve(ln, s)
}

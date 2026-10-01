// Command publish-release publishes the collector release tagged on HEAD to
// the platform's publish API (package publish). Run it through
// scripts/publish_release.sh; docs/deployment.md documents its usage.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"

	"dbcheck/reporter/internal/publish"
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("publish-release", flag.ContinueOnError)
	fs.SetOutput(stderr)
	url := fs.String("url", getenv("DBCHECK_PUBLISH_URL"), "the platform's base URL, e.g. https://dbcheck.example.com (or env DBCHECK_PUBLISH_URL)")
	repo := fs.String("repo", ".", "the git checkout to publish from")
	dist := fs.String("dist-dir", "", "where the release packages are built (default <repo>/dist)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *url == "" {
		fmt.Fprintln(stderr, "[ERROR] --url (or DBCHECK_PUBLISH_URL) is required")
		return 2
	}
	if *dist == "" {
		*dist = filepath.Join(*repo, "dist")
	}
	distDir, err := filepath.Abs(*dist)
	if err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	res, err := publish.Run(ctx, publish.Options{
		RepoDir: *repo, DistDir: distDir, BaseURL: *url, Token: getenv("DBCHECK_PUBLISH_TOKEN"),
	})
	if err != nil {
		fmt.Fprintf(stderr, "[ERROR] %v\n", err)
		return 1
	}
	r := res.Release
	if res.Created {
		fmt.Fprintf(stdout, "[INFO] published collector release %s (%s) from %s at %s\n", r.Version, r.Status, r.Tag, r.Commit)
	} else {
		fmt.Fprintf(stdout, "[INFO] collector release %s was already published with identical content; nothing changed (status %s)\n", r.Version, r.Status)
	}
	return 0
}

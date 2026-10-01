package web

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"dbcheck/reporter/internal/store"
)

func Run(cfg Config) error {
	if err := ensureDir(cfg.DataDir); err != nil {
		return err
	}
	db, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer db.Close()
	handler, err := NewHandler(cfg, Platform{DB: db})
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:    cfg.Addr,
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-stop:
		fmt.Printf("[INFO] received signal: %s\n", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout())
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-errCh:
		return err
	}
}

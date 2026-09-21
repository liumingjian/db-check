package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func Run(cfg Config) error {
	if err := ensureDir(cfg.DataDir); err != nil {
		return err
	}
	lifecycle, err := NewTaskLifecycle(cfg)
	if err != nil {
		return err
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		_ = lifecycle.Close(context.Background())
		return err
	}
	server := &http.Server{
		Addr:    cfg.Addr,
		Handler: newAPIHandlerWithLifecycle(cfg, lifecycle).handler(),
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
		return errors.Join(server.Shutdown(ctx), lifecycle.Close(ctx))
	case err := <-errCh:
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout())
		defer cancel()
		return errors.Join(err, lifecycle.Close(ctx))
	}
}

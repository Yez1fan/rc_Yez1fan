// Command notifier is the entrypoint for the API notification service. It wires
// the durable SQLite store, the retrying dispatcher, and the HTTP API, then runs
// them until an interrupt triggers a graceful shutdown.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"rc_Yez1fan/internal/httpapi"
	"rc_Yez1fan/internal/notifier"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	addr := env("NOTIFIER_ADDR", ":8080")
	dbPath := env("NOTIFIER_DB", "notifier.db")

	store, err := notifier.OpenSQLite(dbPath)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	cfg := notifier.Config{
		Workers:        envInt("NOTIFIER_WORKERS", 4),
		RequestTimeout: time.Duration(envInt("NOTIFIER_REQUEST_TIMEOUT_MS", 10000)) * time.Millisecond,
		MaxAttempts:    envInt("NOTIFIER_MAX_ATTEMPTS", 8),
	}
	dispatcher := notifier.NewDispatcher(store, cfg, log)

	// Root context cancelled on SIGINT/SIGTERM drives a coordinated shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dispatcherDone := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(dispatcherDone)
	}()

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(store, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http listening", "addr", addr, "db", dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http serve", "err", err)
			stop() // trigger shutdown if the listener dies
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Stop accepting new work first, then let in-flight deliveries drain.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	<-dispatcherDone
	log.Info("bye")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

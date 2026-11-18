package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/tyspice/spicedb/internal/engine/lsm"
)

func main() {
	mx := http.NewServeMux()

	mx.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello client"))
		if err != nil {
			fmt.Errorf("Get response: %w", err)
		}
	})

	s := &http.Server{
		Addr:    ":8080",
		Handler: mx,
	}

	fmt.Println("starting storage engine")
	engine, err := lsm.Open("./tmp/data/", lsm.Options{})
	if err != nil {
		fmt.Errorf("could not start storage engine %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Println("starting server on :8080")
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Errorf("listen and serve: %w", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("shutdown signal recieved")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("shutting down server")
	if err := s.Shutdown(shutdownCtx); err != nil {
		fmt.Errorf("graceful shutdown timed out, forcing close: %w", err)
		if closeErr := s.Close(); closeErr != nil {
			fmt.Errorf("force close error: %w", closeErr)
		}
	}

	fmt.Println("shutting down engine")
	if err := engine.Close(); err != nil {
		fmt.Errorf("could not shut down engine: %w", err)
	}
	fmt.Println("engine shutdown succesfully")

	fmt.Println("program shutdown successful\ngoodbye :)")
}

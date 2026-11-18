package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tyspice/spicedb/internal/engine/lsm"
	"github.com/tyspice/spicedb/internal/httpserver"
)

func main() {
	fmt.Println("starting storage engine")
	engine, err := lsm.Open("./tmp/data/", lsm.Options{})
	if err != nil {
		fmt.Errorf("could not start storage engine %w", err)
		os.Exit(1)
	}

	port := ":8080"
	s := httpserver.New(engine, httpserver.Options{Port: port})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Println("starting server on " + port)
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if errors.Is(err, http.ErrServerClosed) {
				fmt.Println("server shut down successfully")
			} else {
				fmt.Errorf("listen and serve: %w", err)
				os.Exit(1)
			}
		}
	}()

	<-ctx.Done()

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
	fmt.Println("engine shut down succesfully")

	fmt.Println("program shut down successful\ngoodbye :)")
}

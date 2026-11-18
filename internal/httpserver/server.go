package httpserver

import (
	"fmt"
	"net/http"
	"path"

	"github.com/tyspice/spicedb/internal/engine"
)

const apiBase = "/db/v1"

var apiSingleton = path.Join(apiBase, "{id}")

type Options struct {
	Port string
}

func New(e engine.Engine, opts Options) *http.Server {
	if opts.Port == "" {
		opts.Port = ":8080"
	}

	mux := http.NewServeMux()

	// for testing
	mux.HandleFunc(http.MethodGet+" /", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("hello client"))
		if err != nil {
			fmt.Errorf("Get / response: %w", err)
		}
	})

	mux.HandleFunc(http.MethodGet+" "+apiSingleton, func(w http.ResponseWriter, r *http.Request) {
	})

	mux.HandleFunc(http.MethodPost+" "+apiSingleton, func(w http.ResponseWriter, r *http.Request) {
	})

	mux.HandleFunc(http.MethodDelete+" "+apiSingleton, func(w http.ResponseWriter, r *http.Request) {
	})

	return &http.Server{
		Addr:    opts.Port,
		Handler: mux,
	}
}

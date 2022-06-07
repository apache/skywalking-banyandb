package http

import (
	"fmt"
	"io/fs"
	stdhttp "net/http"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/ui"
)

type ServiceRepo interface {
	run.Config
	run.Service
}

var (
	_ ServiceRepo = (*service)(nil)
)

func NewService() ServiceRepo {
	return &service{
		stopCh: make(chan struct{}),
	}
}

type service struct {
	listenAddr string
	mux        *stdhttp.ServeMux
	stopCh     chan struct{}
	l          *logger.Logger
}

func (p *service) FlagSet() *run.FlagSet {
	flagSet := run.NewFlagSet("")
	flagSet.StringVar(&p.listenAddr, "http-addr", ":17913", "listen addr for http")
	return flagSet
}

func (p *service) Validate() error {
	return nil
}

func (p *service) Name() string {
	return "liaison-http"
}

func (p *service) PreRun() error {
	p.l = logger.GetLogger(p.Name())
	fSys, err := fs.Sub(ui.DistContent, "dist")
	if err != nil {
		return err
	}
	p.mux = stdhttp.NewServeMux()
	httpFS := stdhttp.FS(fSys)
	fileServer := stdhttp.FileServer(stdhttp.FS(fSys))
	serveIndex := serveFileContents("index.html", httpFS)
	p.mux.Handle("/", intercept404(fileServer, serveIndex))
	//TODO: add grpc gateway handler
	return nil
}

func (p *service) Serve() run.StopNotify {

	go func() {
		p.l.Info().Str("listenAddr", p.listenAddr).Msg("Start liaison http server")
		_ = stdhttp.ListenAndServe(p.listenAddr, p.mux)
		p.stopCh <- struct{}{}
	}()

	return p.stopCh
}

func (p *service) GracefulStop() {
	close(p.stopCh)
}

func intercept404(handler, on404 stdhttp.Handler) stdhttp.Handler {
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		hookedWriter := &hookedResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(hookedWriter, r)

		if hookedWriter.got404 {
			on404.ServeHTTP(w, r)
		}
	})
}

type hookedResponseWriter struct {
	stdhttp.ResponseWriter
	got404 bool
}

func (hrw *hookedResponseWriter) WriteHeader(status int) {
	if status == stdhttp.StatusNotFound {
		hrw.got404 = true
	} else {
		hrw.ResponseWriter.WriteHeader(status)
	}
}

func (hrw *hookedResponseWriter) Write(p []byte) (int, error) {
	if hrw.got404 {
		return len(p), nil
	}

	return hrw.ResponseWriter.Write(p)
}

func serveFileContents(file string, files stdhttp.FileSystem) stdhttp.HandlerFunc {
	return func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if !strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.WriteHeader(stdhttp.StatusNotFound)
			fmt.Fprint(w, "404 not found")

			return
		}
		index, err := files.Open(file)
		if err != nil {
			w.WriteHeader(stdhttp.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}
		fi, err := index.Stat()
		if err != nil {
			w.WriteHeader(stdhttp.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		stdhttp.ServeContent(w, r, fi.Name(), fi.ModTime(), index)
	}
}

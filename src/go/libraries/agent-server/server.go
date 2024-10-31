package server

import (
	"context"
	"net/http"
	"time"

	handlers "banana/libraries/agent-handlers"

	"github.com/gorilla/mux"
)

type Server struct {
	router *mux.Router
	srv    *http.Server
}

func New(apiKey string, acl string) *Server {
	r := mux.NewRouter()
	handlers.SetupRoutes(r, apiKey, acl)

	return &Server{
		router: r,
	}
}

func (s *Server) Start(addr string) error {
	s.srv = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}
	return s.srv.ListenAndServeTLS("agent_cert.pem", "agent_key.pem")
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

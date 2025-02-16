package middlewares

import (
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/config"
	"net/http"
)

func AuthMiddleware(cfg *config.Config) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			query := r.URL.Query()
			username := query.Get("username")
			password := query.Get("password")

			var valid bool
			for _, user := range cfg.Users {
				if username == user.Username && auth.CheckPassword(password, user.Password) {
					valid = true
					break
				}
			}
			if !valid {
				http.Error(w, `{"error": "invalid credentials"}`, http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

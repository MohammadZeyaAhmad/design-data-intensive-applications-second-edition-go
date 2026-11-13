package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"time"
)

func Guard(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		budget := 10 * time.Second
		if v := r.Header.Values("X-Request-Timeout-Ms"); len(v) > 0 {
			if len(v) != 1 {
				Problem(w, 400, "invalid_timeout")
				return
			}
			n, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil || n < 1 || n > 10000 {
				Problem(w, 400, "invalid_timeout")
				return
			}
			budget = time.Duration(n) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(r.Context(), budget)
		defer cancel()
		defer func() {
			if recover() != nil {
				logger.Error("request panic")
				Problem(w, 500, "internal_error")
			}
		}()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func Decode(w http.ResponseWriter, r *http.Request, target any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		Problem(w, 415, "unsupported_media_type")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err = d.Decode(target); err != nil {
		Problem(w, 400, "invalid_json")
		return false
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		Problem(w, 400, "invalid_json")
		return false
	}
	return true
}
func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func Problem(w http.ResponseWriter, status int, code string) {
	JSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

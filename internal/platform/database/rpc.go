package database

import (
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

var endpointLabel = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

// ConfigureRPC bounds orphaned SQL even when the API process is SIGKILLed.
// Context cancellation also sends PostgreSQL's actual CancelRequest; closing
// the client socket alone need not interrupt a query waiting on a row lock.
func ConfigureRPC(c *pgxpool.Config, label string) error {
	if !endpointLabel.MatchString(label) {
		return errors.New("RPC endpoint must be a lowercase slug of 1..48 characters")
	}
	c.ConnConfig.RuntimeParams["application_name"] = "atlas-rpc/" + label
	c.ConnConfig.RuntimeParams["statement_timeout"] = "2500ms"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "1500ms"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "3000ms"
	configureCancellation(c)
	return nil
}

// ConfigureHTTP leaves longer headroom under HTTP's ten-second request cap.
// Workers/migrations use their own pools and are not subject to API SQL limits.
func ConfigureHTTP(c *pgxpool.Config) {
	c.ConnConfig.RuntimeParams["application_name"] = "atlas-http/local"
	c.ConnConfig.RuntimeParams["statement_timeout"] = "8000ms"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "7000ms"
	c.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "9000ms"
	configureCancellation(c)
}
func configureCancellation(c *pgxpool.Config) {
	c.ConnConfig.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, CancelRequestDelay: 25 * time.Millisecond, DeadlineDelay: 750 * time.Millisecond}
	}
}

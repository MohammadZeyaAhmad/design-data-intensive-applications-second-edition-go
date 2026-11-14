package database

import (
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConfigureWriterEpoch sets a server-owned deployment token on every connection.
// An empty token preserves deployments where the Inventory fence is disabled.
// This is a stale-process fence, not authentication or a leader election system.
func ConfigureWriterEpoch(c *pgxpool.Config, epoch string) error {
	if epoch == "" {
		return nil
	}
	n, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != epoch {
		return errors.New("ATLAS_DATABASE_WRITER_EPOCH must be a canonical positive int64")
	}
	c.ConnConfig.RuntimeParams["atlas.writer_epoch"] = epoch
	return nil
}

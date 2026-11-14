package database

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWriterEpochConfiguration(t *testing.T) {
	for _, token := range []string{"", "1", "9223372036854775807", "0", "-1", "+1", "01", " 1", "1;RESET ALL", "9223372036854775808"} {
		c, err := pgxpool.ParseConfig("postgres://localhost/atlas")
		if err != nil {
			t.Fatal(err)
		}
		err = ConfigureWriterEpoch(c, token)
		valid := token == "" || token == "1" || token == "9223372036854775807"
		if (err == nil) != valid {
			t.Fatalf("token %q: %v", token, err)
		}
		if valid && c.ConnConfig.RuntimeParams["atlas.writer_epoch"] != token {
			t.Fatal("connection token missing")
		}
	}
}

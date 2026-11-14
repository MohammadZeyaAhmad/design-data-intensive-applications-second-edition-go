package logformats

import (
	"context"
	"errors"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/internal/pgprocess"
)

var ErrPhysical = errors.New("physical WAL contract requires matching cluster, major, catalog and block format")

type PhysicalContract struct {
	SystemID       string `json:"system_id"`
	Major          int    `json:"major"`
	BlockSize      int    `json:"block_size"`
	CatalogVersion int64  `json:"catalog_version"`
}

func CompatiblePhysical(source, target PhysicalContract) error {
	if source.SystemID == "" || source.Major <= 0 || source.BlockSize <= 0 || source != target {
		return ErrPhysical
	}
	return nil
}
func physicalContract(ctx context.Context, c *pgprocess.Cluster, n *pgprocess.Node) (PhysicalContract, error) {
	conn, err := c.Connect(n)
	if err != nil {
		return PhysicalContract{}, err
	}
	defer conn.Close(context.Background())
	var p PhysicalContract
	err = conn.QueryRow(ctx, `SELECT system_identifier::text,current_setting('server_version_num')::int/10000,current_setting('block_size')::int,catalog_version_no FROM pg_control_system()`).Scan(&p.SystemID, &p.Major, &p.BlockSize, &p.CatalogVersion)
	return p, err
}

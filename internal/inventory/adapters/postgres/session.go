package postgres

import (
	"context"
	"errors"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/readrouting"
	"github.com/jackc/pgx/v5"
)

var _ ports.SessionReader = (*Store)(nil)

func (s *Store) SessionEnabled() bool { return s.ReadRouter != nil }
func (s *Store) VerifySession(ctx context.Context, t, token string) error {
	return s.ReadRouter.Verify(ctx, t, token)
}
func (s *Store) AfterWrite(ctx context.Context, t, token string) (ports.ReadMeta, error) {
	m, e := s.ReadRouter.AfterWrite(ctx, t, token)
	return ports.ReadMeta(m), e
}
func (s *Store) QueryView(ctx context.Context, t string, q ports.ViewQuery, o ports.ReadOptions) (ports.View, ports.ReadMeta, error) {
	var view ports.View
	var observation error
	meta, err := s.ReadRouter.Read(ctx, t, readrouting.ReadOptions(o), func(ctx context.Context, tx pgx.Tx) error {
		if q.SKU != "" {
			stock, e := scanStock(tx.QueryRow(ctx, `SELECT tenant_id,sku,on_hand,reserved,allocated,version,warehouse_id FROM inventory.stock WHERE tenant_id=$1 AND sku=$2 AND warehouse_id=$3`, t, q.SKU, dbWarehouse(q.WarehouseID)))
			if errors.Is(e, domain.ErrNotFound) {
				observation = e
				return nil
			}
			if e != nil {
				return e
			}
			view.Stock = &stock
		}
		if q.ReservationID != "" {
			res, e := scanReservation(tx.QueryRow(ctx, `SELECT id::text,tenant_id,sku,quantity,status,version,expires_at,warehouse_id FROM inventory.reservations WHERE tenant_id=$1 AND id=$2::uuid`, t, q.ReservationID))
			if errors.Is(e, domain.ErrNotFound) {
				observation = e
				return nil
			}
			if e != nil {
				return e
			}
			if q.SKU != "" && (res.SKU != q.SKU || dbWarehouse(res.WarehouseID) != dbWarehouse(q.WarehouseID)) {
				observation = domain.ErrNotFound
				return nil
			}
			view.Reservation = &res
		}
		return nil
	})
	if err != nil {
		return ports.View{}, ports.ReadMeta{}, err
	}
	return view, ports.ReadMeta(meta), observation
}

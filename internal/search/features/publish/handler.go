package publish

import (
	"context"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/ports"
)

type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, d domain.Document) (domain.Document, bool, error) {
	if !d.Valid() {
		return domain.Document{}, false, domain.ErrInvalid
	}
	r, e := commands.New(tenant, "publish-document", key, d)
	if e != nil {
		return domain.Document{}, false, e
	}
	return h.Store.Publish(ctx, r, d)
}
func (h Handler) Delete(ctx context.Context, tenant, key, id string, expected int64) (domain.Document, bool, error) {
	if !domain.ValidID(id) || expected < 0 || expected >= 1e12 {
		return domain.Document{}, false, domain.ErrInvalid
	}
	payload := struct {
		ID       string
		Expected int64
	}{id, expected}
	r, e := commands.New(tenant, "remove-document", key, payload)
	if e != nil {
		return domain.Document{}, false, e
	}
	return h.Store.Remove(ctx, r, id, expected)
}

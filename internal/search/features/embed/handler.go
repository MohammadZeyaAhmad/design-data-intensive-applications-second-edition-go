package embed

import (
	"context"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/ports"
)

type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, d domain.Embedding) (domain.Embedding, bool, error) {
	if !d.Valid() {
		return domain.Embedding{}, false, domain.ErrInvalid
	}
	r, e := commands.New(tenant, "publish-embedding", key, d)
	if e != nil {
		return domain.Embedding{}, false, e
	}
	return h.Store.Embed(ctx, r, d)
}

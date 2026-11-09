package publish

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Handler struct{ Store ports.Store }

func (h Handler) Handle(ctx context.Context, tenant, key string, p domain.Product) (domain.Product, bool, error) {
	if !domain.ValidProduct(p) {
		return domain.Product{}, false, domain.ErrInvalid
	}
	r, err := commands.New(tenant, "publish-product", key, p)
	if err != nil {
		return domain.Product{}, false, err
	}
	return h.Store.Publish(ctx, r, p)
}

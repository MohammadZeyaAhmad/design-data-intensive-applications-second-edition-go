package describe

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Handler struct{ Store ports.Content }

func (h Handler) Handle(ctx context.Context, tenant, key string, d domain.Details) (domain.Details, bool, error) {
	if !domain.ValidDetails(d) {
		return domain.Details{}, false, domain.ErrInvalid
	}
	d.BrandName = ""
	r, e := commands.New(tenant, "describe-product", key, d)
	if e != nil {
		return domain.Details{}, false, e
	}
	return h.Store.Describe(ctx, r, d)
}
func (h Handler) Brand(ctx context.Context, tenant, key string, b domain.Brand) (domain.Brand, bool, error) {
	if !domain.ValidBrand(b) {
		return domain.Brand{}, false, domain.ErrInvalid
	}
	r, e := commands.New(tenant, "publish-brand", key, b)
	if e != nil {
		return domain.Brand{}, false, e
	}
	return h.Store.PublishBrand(ctx, r, b)
}

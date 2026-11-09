package quote

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Handler struct{ Store ports.Store }
type Command struct {
	Lines []domain.Line `json:"lines"`
}

func (h Handler) Handle(ctx context.Context, tenant, key string, c Command) (catalog.Quote, bool, error) {
	if !domain.ValidLines(c.Lines) {
		return catalog.Quote{}, false, domain.ErrInvalid
	}
	r, err := commands.New(tenant, "create-quote", key, c)
	if err != nil {
		return catalog.Quote{}, false, err
	}
	return h.Store.CreateQuote(ctx, r, c.Lines)
}

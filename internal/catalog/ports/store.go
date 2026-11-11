package ports

import (
	"context"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/catalog/domain"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
)

type Store interface {
	Publish(context.Context, commands.Request, domain.Product) (domain.Product, bool, error)
	CreateQuote(context.Context, commands.Request, []domain.Line) (catalog.Quote, bool, error)
	Quote(context.Context, string, string) (catalog.Quote, error)
	Product(context.Context, string, string) (domain.Product, error)
}
type Content interface {
	Brand(context.Context, string, string) (domain.Brand, error)
	PublishBrand(context.Context, commands.Request, domain.Brand) (domain.Brand, bool, error)
	Describe(context.Context, commands.Request, domain.Details) (domain.Details, bool, error)
	Details(context.Context, string, string) (domain.Details, error)
}

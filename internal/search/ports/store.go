package ports

import (
	"context"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/search"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/commands"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/search/domain"
)

type Store interface {
	Publish(context.Context, commands.Request, domain.Document) (domain.Document, bool, error)
	Remove(context.Context, commands.Request, string, int64) (domain.Document, bool, error)
	Document(context.Context, string, string) (domain.Document, error)
	Text(context.Context, string, string, int) ([]api.Hit, error)
	Suggest(context.Context, string, string, int) ([]api.Hit, error)
	Box(context.Context, string, domain.Box, int) ([]api.Hit, error)
	Radius(context.Context, string, domain.Point, float64, int) ([]api.Hit, error)
	Embed(context.Context, commands.Request, domain.Embedding) (domain.Embedding, bool, error)
	Vector(context.Context, string, string, string, []float64, int, string) ([]api.Hit, error)
}

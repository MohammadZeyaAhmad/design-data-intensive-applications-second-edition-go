package ports

import (
	"context"
	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/payments"
)

type Provider interface {
	Get(context.Context, api.Intent) (api.Receipt, error)
	Capture(context.Context, api.Intent) (api.Receipt, error)
	Close(context.Context, api.Intent) (api.Receipt, error)
}

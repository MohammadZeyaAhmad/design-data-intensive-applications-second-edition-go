package catalog

import "context"

type ProductView struct {
	TenantID       string
	SKU            string
	Name           string
	UnitPriceMinor int64
	Currency       string
	Version        int64
}
type VariantView struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}
type BrandView struct{ TenantID, ID, Name string }
type DetailView struct {
	TenantID, SKU, Description string
	Version                    int64
	Brand                      BrandView
	Categories                 []string
	Variants                   []VariantView
}

// Read batches are bounded and tenant scoped. Omitted identities are missing;
// infrastructure failures fail the batch rather than masquerading as misses.
type ProductReader interface {
	ReadProducts(context.Context, string, []string) ([]ProductView, error)
	ReadProductDetails(context.Context, string, []string) ([]DetailView, error)
}

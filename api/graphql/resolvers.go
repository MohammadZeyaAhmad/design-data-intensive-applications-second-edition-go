package graphql

import (
	"context"
	"strings"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/catalog"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/ordering"
	engine "github.com/graph-gophers/graphql-go"
)

type resolver struct{}

func (*resolver) Order(ctx context.Context, args struct {
	ID         engine.ID
	MinVersion *Long
}) (*orderResolver, error) {
	id := strings.ToLower(string(args.ID))
	if !orderID.MatchString(id) || args.MinVersion != nil && *args.MinVersion < 0 {
		return nil, badInput()
	}
	s := request(ctx)
	if e := s.permit(ctx, "order", id, "projection.read"); e != nil {
		return nil, e
	}
	rows, e := s.orders.load(ctx, []string{id})
	if e != nil {
		return nil, e
	}
	v := rows[id]
	if v == nil {
		return nil, nil
	}
	if args.MinVersion != nil && v.Version < int64(*args.MinVersion) {
		return nil, &publicError{"STALE_PROJECTION", "projection has not reached the requested version"}
	}
	if e = s.prepare(ctx, []ordering.Projection{*v}, ""); e != nil {
		return nil, e
	}
	return &orderResolver{*v, s}, nil
}
func (*resolver) Orders(ctx context.Context, args struct {
	First int32
	After *string
}) (*connectionResolver, error) {
	if args.First < 1 || args.First > 20 {
		return nil, badInput()
	}
	s := request(ctx)
	after := ""
	if args.After != nil {
		var e error
		after, e = s.codec.decode(s.tenant, *args.After)
		if e != nil {
			return nil, e
		}
	}
	if e := s.permit(ctx, "order", "*", "projection.list"); e != nil {
		return nil, e
	}
	page, e := s.options.Orders.ListOrderProjections(ctx, s.tenant, after, int(args.First))
	if e != nil {
		return nil, unavailable()
	}
	if len(page.Nodes) > int(args.First) {
		return nil, unavailable()
	}
	last := after
	for _, p := range page.Nodes {
		if !validProjection(p, s.tenant) || p.ID <= last {
			return nil, forbidden()
		}
		if e := s.permit(ctx, "order", p.ID, "projection.read"); e != nil {
			return nil, e
		}
		last = p.ID
	}
	if e = s.prepare(ctx, page.Nodes, "edges.node."); e != nil {
		return nil, e
	}
	return &connectionResolver{page, s}, nil
}
func (*resolver) Product(ctx context.Context, args struct{ SKU string }) (*productResolver, error) {
	if !skuID.MatchString(args.SKU) {
		return nil, badInput()
	}
	s := request(ctx)
	if e := s.permit(ctx, "product", args.SKU, "catalog.read"); e != nil {
		return nil, e
	}
	rows, e := s.products.load(ctx, []string{args.SKU})
	if e != nil {
		return nil, e
	}
	p := rows[args.SKU]
	if p == nil {
		return nil, nil
	}
	if engine.HasSelectedField(ctx, "details") && s.permit(ctx, "product", p.SKU, "catalog.details.read") == nil {
		if _, e = s.details.load(ctx, []string{p.SKU}); e != nil {
			return nil, e
		}
	}
	return &productResolver{*p, s}, nil
}
func validProjection(p ordering.Projection, tenant string) bool {
	if p.TenantID != tenant || !orderID.MatchString(p.ID) || p.Version < 1 || len(p.Lines) < 1 || len(p.Lines) > 100 {
		return false
	}
	seen := map[string]bool{}
	for _, l := range p.Lines {
		if !skuID.MatchString(l.SKU) || l.Quantity < 1 || l.Quantity > 1000000000 || seen[l.SKU] {
			return false
		}
		seen[l.SKU] = true
	}
	return true
}
func (s *requestScope) prepare(ctx context.Context, orders []ordering.Projection, prefix string) error {
	if !engine.HasSelectedField(ctx, prefix+"lines.product") {
		return nil
	}
	keys := []string{}
	seen := map[string]bool{}
	for _, order := range orders {
		for _, line := range order.Lines {
			if s.permit(ctx, "product", line.SKU, "catalog.read") != nil {
				continue
			}
			if !seen[line.SKU] {
				keys = append(keys, line.SKU)
				seen[line.SKU] = true
			}
		}
	}
	products, e := s.products.load(ctx, keys)
	if e != nil {
		return e
	}
	if engine.HasSelectedField(ctx, prefix+"lines.product.details") {
		existing := []string{}
		for _, key := range keys {
			if products[key] != nil && s.permit(ctx, "product", key, "catalog.details.read") == nil {
				existing = append(existing, key)
			}
		}
		_, e = s.details.load(ctx, existing)
	}
	return e
}

type orderResolver struct {
	v ordering.Projection
	s *requestScope
}

func (o *orderResolver) ID() engine.ID  { return engine.ID(o.v.ID) }
func (o *orderResolver) Version() Long  { return Long(o.v.Version) }
func (o *orderResolver) Status() string { return o.v.Status }
func (o *orderResolver) Currency() *string {
	if o.v.Currency == "" {
		return nil
	}
	return &o.v.Currency
}
func (o *orderResolver) Lines() []*lineResolver {
	out := make([]*lineResolver, 0, len(o.v.Lines))
	for _, l := range o.v.Lines {
		out = append(out, &lineResolver{l, o.v.ID, o.s})
	}
	return out
}

type lineResolver struct {
	v     ordering.ProjectionLine
	order string
	s     *requestScope
}

func (l *lineResolver) SKU() string     { return l.v.SKU }
func (l *lineResolver) Quantity() int32 { return int32(l.v.Quantity) }
func (l *lineResolver) Product(ctx context.Context) (*productResolver, error) {
	if e := l.s.permit(ctx, "product", l.v.SKU, "catalog.read"); e != nil {
		return nil, e
	}
	rows, e := l.s.products.load(ctx, []string{l.v.SKU})
	if e != nil {
		return nil, e
	}
	if p := rows[l.v.SKU]; p != nil {
		return &productResolver{*p, l.s}, nil
	}
	return nil, nil
}
func (l *lineResolver) FrozenUnitPriceMinor(ctx context.Context) (*Long, error) {
	if e := l.s.permit(ctx, "order", l.order, "order.pricing.read"); e != nil {
		return nil, e
	}
	return longPointer(l.v.FrozenUnitPriceMinor), nil
}
func (l *lineResolver) FrozenPriceVersion(ctx context.Context) (*Long, error) {
	if e := l.s.permit(ctx, "order", l.order, "order.pricing.read"); e != nil {
		return nil, e
	}
	return longPointer(l.v.FrozenPriceVersion), nil
}
func longPointer(p *int64) *Long {
	if p == nil {
		return nil
	}
	v := Long(*p)
	return &v
}

type productResolver struct {
	v catalog.ProductView
	s *requestScope
}

func (p *productResolver) SKU() string      { return p.v.SKU }
func (p *productResolver) Name() string     { return p.v.Name }
func (p *productResolver) Version() Long    { return Long(p.v.Version) }
func (p *productResolver) Currency() string { return p.v.Currency }
func (p *productResolver) UnitPriceMinor(ctx context.Context) (*Long, error) {
	if e := p.s.permit(ctx, "product", p.v.SKU, "catalog.price.read"); e != nil {
		return nil, e
	}
	n := Long(p.v.UnitPriceMinor)
	return &n, nil
}
func (p *productResolver) PriceMinor(ctx context.Context) (*Long, error) {
	return p.UnitPriceMinor(ctx)
}
func (p *productResolver) Details(ctx context.Context) (*detailsResolver, error) {
	if e := p.s.permit(ctx, "product", p.v.SKU, "catalog.details.read"); e != nil {
		return nil, e
	}
	rows, e := p.s.details.load(ctx, []string{p.v.SKU})
	if e != nil {
		return nil, e
	}
	if d := rows[p.v.SKU]; d != nil {
		return &detailsResolver{*d, p.s}, nil
	}
	return nil, nil
}

type detailsResolver struct {
	v catalog.DetailView
	s *requestScope
}

func (d *detailsResolver) Revision() Long      { return Long(d.v.Version) }
func (d *detailsResolver) Description() string { return d.v.Description }
func (d *detailsResolver) Brand(ctx context.Context) (*brandResolver, error) {
	if e := d.s.permit(ctx, "brand", d.v.Brand.ID, "catalog.brand.read"); e != nil {
		return nil, e
	}
	return &brandResolver{d.v.Brand}, nil
}
func (d *detailsResolver) Categories() []string {
	if d.v.Categories == nil {
		return []string{}
	}
	return d.v.Categories
}
func (d *detailsResolver) Variants() []*variantResolver {
	out := []*variantResolver{}
	for _, v := range d.v.Variants {
		out = append(out, &variantResolver{v})
	}
	return out
}

type brandResolver struct{ v catalog.BrandView }

func (b *brandResolver) ID() string   { return b.v.ID }
func (b *brandResolver) Name() string { return b.v.Name }

type variantResolver struct{ v catalog.VariantView }

func (v *variantResolver) Code() string  { return v.v.Code }
func (v *variantResolver) Label() string { return v.v.Label }

type connectionResolver struct {
	v ordering.ProjectionPage
	s *requestScope
}

func (c *connectionResolver) Edges() []*edgeResolver {
	out := []*edgeResolver{}
	for _, p := range c.v.Nodes {
		out = append(out, &edgeResolver{p, c.s})
	}
	return out
}
func (c *connectionResolver) PageInfo() *pageResolver {
	var cursor *string
	if len(c.v.Nodes) > 0 {
		v := c.s.codec.encode(c.s.tenant, c.v.Nodes[len(c.v.Nodes)-1].ID)
		cursor = &v
	}
	return &pageResolver{cursor, c.v.HasNextPage}
}

type edgeResolver struct {
	v ordering.Projection
	s *requestScope
}

func (e *edgeResolver) Cursor() string       { return e.s.codec.encode(e.s.tenant, e.v.ID) }
func (e *edgeResolver) Node() *orderResolver { return &orderResolver{e.v, e.s} }

type pageResolver struct {
	cursor *string
	more   bool
}

func (p *pageResolver) EndCursor() *string { return p.cursor }
func (p *pageResolver) HasNextPage() bool  { return p.more }

package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
)

type graphqlReply struct {
	Data struct {
		Order *struct {
			ID, Version string
			Lines       []struct {
				SKU      string
				Quantity int64
				Product  *struct {
					Name, UnitPriceMinor string
					Details              *struct{ Brand struct{ ID, Name string } }
				}
			}
		}
		Product *struct{ Name string }
		Orders  struct {
			PageInfo struct {
				EndCursor   string
				HasNextPage bool
			}
		}
	}
	Errors []struct{ Extensions struct{ Code string } }
}

func graphqlRead(c *client, tenant, query string, variables map[string]any) (graphqlReply, error) {
	var out graphqlReply
	b, e := c.expect("POST", "/v1/graphql", tenant, "", map[string]any{"query": query, "variables": variables}, 200)
	if e != nil {
		return out, e
	}
	if json.Unmarshal(b, &out) != nil {
		return out, errors.New("invalid GraphQL response")
	}
	return out, nil
}
func verifyGraphQLLive(c *client) error {
	id, e := identity.NewID()
	if e != nil {
		return e
	}
	sku := "gql-" + id
	brand := "maker-" + id
	path := "/v1/catalog/products/" + sku
	if _, e = c.expect("PUT", "/v1/catalog/brands/"+brand, "acme", id+"-brand", map[string]any{"name": "GraphQL maker", "expected_version": 0}, 200); e != nil {
		return e
	}
	product := map[string]any{"name": "GraphQL original", "unit_price_minor": 1000, "currency": "USD", "expected_version": 0}
	if _, e = c.expect("PUT", path, "acme", id+"-price", product, 200); e != nil {
		return e
	}
	if _, e = c.expect("PUT", path+"/details", "acme", id+"-details", map[string]any{"brand_id": brand, "description": "Read composition", "attributes": map[string]any{}, "variants": []any{}, "categories": []string{"guides"}, "expected_version": 0}, 200); e != nil {
		return e
	}
	wire, e := c.expect("POST", "/v1/orders", "acme", id+"-place", orderCommand(sku, 2), 201)
	if e != nil {
		return e
	}
	state, e := decodeOrder(wire)
	if e != nil {
		return e
	}
	query := `query($id:ID!){order(id:$id,minVersion:"1"){id version lines{sku quantity product{name unitPriceMinor details{brand{id name}}}}}}`
	if e = waitKafkaCheck(func() (bool, error) {
		r, e := graphqlRead(c, "acme", query, map[string]any{"id": state.ID})
		if e != nil {
			return false, e
		}
		if len(r.Errors) > 0 {
			return false, errors.New("GraphQL projection read failed")
		}
		return r.Data.Order != nil && r.Data.Order.Version == "1", nil
	}); e != nil {
		return e
	}
	r, e := graphqlRead(c, "acme", query, map[string]any{"id": state.ID})
	if e != nil {
		return e
	}
	if len(r.Errors) > 0 || r.Data.Order == nil || len(r.Data.Order.Lines) != 1 {
		return errors.New("GraphQL order composition failed")
	}
	line := r.Data.Order.Lines[0]
	if line.SKU != sku || line.Quantity != 2 || line.Product == nil || line.Product.UnitPriceMinor != "1000" || line.Product.Details == nil || line.Product.Details.Brand.Name != "GraphQL maker" {
		return errors.New("GraphQL nested Catalog data differs")
	}
	other, e := graphqlRead(c, "globex", query, map[string]any{"id": state.ID})
	if e != nil || other.Data.Order != nil || len(other.Errors) > 0 {
		return errors.New("GraphQL order tenant scope failed")
	}
	other, e = graphqlRead(c, "globex", `query($sku:String!){product(sku:$sku){name}}`, map[string]any{"sku": sku})
	if e != nil || other.Data.Product != nil || len(other.Errors) > 0 {
		return errors.New("GraphQL Catalog tenant scope failed")
	}
	product["name"], product["unit_price_minor"], product["expected_version"] = "GraphQL revised", 2000, 1
	if _, e = c.expect("PUT", path, "acme", id+"-reprice", product, 200); e != nil {
		return e
	}
	r, e = graphqlRead(c, "acme", query, map[string]any{"id": state.ID})
	if e != nil || len(r.Errors) > 0 || r.Data.Order == nil || r.Data.Order.Lines[0].Product.Name != "GraphQL revised" || r.Data.Order.Lines[0].Product.UnitPriceMinor != "2000" {
		return errors.New("GraphQL request cache retained an old product")
	}
	page, e := graphqlRead(c, "acme", `{orders(first:1){edges{node{id}}pageInfo{endCursor hasNextPage}}}`, nil)
	if e != nil || len(page.Errors) > 0 || page.Data.Orders.PageInfo.EndCursor == "" {
		return errors.New("GraphQL keyset page failed")
	}
	other, e = graphqlRead(c, "globex", `query($after:String){orders(first:1,after:$after){pageInfo{endCursor}}}`, map[string]any{"after": page.Data.Orders.PageInfo.EndCursor})
	if e != nil || len(other.Errors) != 1 || other.Errors[0].Extensions.Code != "BAD_USER_INPUT" {
		return errors.New("GraphQL cursor crossed tenant scope")
	}
	if _, e = c.expect("POST", "/v1/graphql", "acme", "", map[string]any{"query": `mutation{__typename}`}, 400); e != nil {
		return e
	}
	fmt.Println("GraphQL HTTP verification passed: composed projections/current Catalog, request-local freshness, tenant scopes, signed pagination and read-only operations.")
	return nil
}

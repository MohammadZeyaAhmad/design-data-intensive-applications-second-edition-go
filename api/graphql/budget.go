package graphql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

const MaxQueryBytes = 16384
const MaxDepth = 8
const MaxSelections = 512
const MaxCost = 20000
const MaxRoots = 8

type Budget struct{ Depth, Selections, Cost, Roots int }

// Analyze bounds cardinality-weighted work before any resolver or SQL executes.
// Directives are charged conservatively, including skipped selections. Aliases
// and repeated fragment occurrences are charged separately rather than merged.
func Analyze(query, operation string, variables map[string]any) (Budget, error) {
	var b Budget
	if len(query) == 0 || len(query) > MaxQueryBytes || !utf8.ValidString(query) {
		return b, badInput()
	}
	doc, e := parser.ParseQueryWithTokenLimit(&ast.Source{Input: query}, 2048)
	if e != nil {
		return b, badInput()
	}
	if len(doc.Operations) == 0 || len(doc.Operations) > 8 || len(doc.Fragments) > 32 {
		return b, badInput()
	}
	var op *ast.OperationDefinition
	if operation == "" {
		if len(doc.Operations) != 1 {
			return b, badInput()
		}
		op = doc.Operations[0]
	} else {
		op = doc.Operations.ForName(operation)
	}
	if op == nil || op.Operation != ast.Query {
		return b, &publicError{"READ_ONLY", "only query operations are supported"}
	}
	vars := map[string]any{}
	for k, v := range variables {
		vars[k] = v
	}
	for _, v := range op.VariableDefinitions {
		if _, ok := vars[v.Variable]; !ok && v.DefaultValue != nil {
			x, e := v.DefaultValue.Value(nil)
			if e != nil {
				return b, badInput()
			}
			vars[v.Variable] = x
		}
	}
	stack := map[string]bool{}
	var walk func(ast.SelectionSet, string, int, int, int, int) error
	walk = func(selections ast.SelectionSet, parent string, depth, copies, pageSize, expansions int) error {
		if expansions > 32 {
			return badInput()
		}
		for _, selection := range selections {
			b.Selections++
			if b.Selections > MaxSelections {
				return costError()
			}
			switch s := selection.(type) {
			case *ast.FragmentSpread:
				f := doc.Fragments.ForName(s.Name)
				if f == nil || stack[s.Name] {
					return badInput()
				}
				stack[s.Name] = true
				if e := walk(f.SelectionSet, parent, depth, copies, pageSize, expansions+1); e != nil {
					return e
				}
				delete(stack, s.Name)
			case *ast.InlineFragment:
				if e := walk(s.SelectionSet, parent, depth, copies, pageSize, expansions+1); e != nil {
					return e
				}
			case *ast.Field:
				d := depth + 1
				if d > b.Depth {
					b.Depth = d
				}
				if d > MaxDepth {
					return costError()
				}
				if s.Name == "__schema" || s.Name == "__type" {
					return &publicError{"INTROSPECTION_DISABLED", "schema introspection is disabled"}
				}
				if parent == "Query" {
					b.Roots++
					if b.Roots > MaxRoots {
						return costError()
					}
				}
				weight := fieldWeight(parent, s.Name)
				if copies > MaxCost/weight {
					return costError()
				}
				b.Cost += copies * weight
				if b.Cost > MaxCost {
					return costError()
				}
				child, multiplier := childType(parent, s.Name), 1
				switch parent + "." + s.Name {
				case "Query.orders":
					pageSize = 10
					if arg := s.Arguments.ForName("first"); arg != nil {
						_, supplied := vars[arg.Value.Raw]
						if arg.Value.Kind != ast.Variable || supplied {
							v, e := arg.Value.Value(vars)
							if e != nil {
								return badInput()
							}
							pageSize, e = integer(v)
							if e != nil {
								return e
							}
						}
					}
					if pageSize < 1 || pageSize > 20 {
						return badInput()
					}
				case "OrderConnection.edges":
					multiplier = pageSize
				case "Order.lines":
					multiplier = 100
				case "ProductDetails.variants":
					multiplier = 32
				case "ProductDetails.categories":
					multiplier = 16
				}
				if copies > MaxCost/multiplier {
					return costError()
				}
				if e := walk(s.SelectionSet, child, d, copies*multiplier, pageSize, expansions); e != nil {
					return e
				}
			}
		}
		return nil
	}
	e = walk(op.SelectionSet, "Query", 0, 1, 10, 0)
	return b, e
}
func childType(parent, field string) string {
	switch parent + "." + field {
	case "Query.order", "OrderEdge.node":
		return "Order"
	case "Query.orders":
		return "OrderConnection"
	case "OrderConnection.edges":
		return "OrderEdge"
	case "OrderConnection.pageInfo":
		return "PageInfo"
	case "Order.lines":
		return "OrderLine"
	case "OrderLine.product", "Query.product":
		return "Product"
	case "Product.details":
		return "ProductDetails"
	case "ProductDetails.brand":
		return "Brand"
	case "ProductDetails.variants":
		return "Variant"
	}
	return ""
}

// Scalar weights reflect the source contract's maximum string size in roughly
// 64-byte units. Scalar lists need a direct cardinality charge even though they
// have no child selection set.
func fieldWeight(parent, field string) int {
	switch parent + "." + field {
	case "ProductDetails.description":
		return 64
	case "Product.name", "Brand.name":
		return 4
	case "Variant.label":
		return 2
	case "ProductDetails.categories":
		return 16
	}
	return 1
}
func integer(v any) (int, error) {
	switch n := v.(type) {
	case int64:
		if n >= 1 && n <= 20 {
			return int(n), nil
		}
	case float64:
		if n >= 1 && n <= 20 && n == float64(int(n)) {
			return int(n), nil
		}
	case int:
		if n >= 1 && n <= 20 {
			return n, nil
		}
	case json.Number:
		i, e := strconv.Atoi(string(n))
		if e == nil && i >= 1 && i <= 20 {
			return i, nil
		}
	}
	return 0, badInput()
}
func costError() error { return &publicError{"QUERY_LIMIT", "query exceeds the configured work limit"} }
func (b Budget) String() string {
	return fmt.Sprintf("depth=%d selections=%d cost=%d roots=%d", b.Depth, b.Selections, b.Cost, b.Roots)
}

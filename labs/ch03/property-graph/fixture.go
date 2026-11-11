package graph

import "fmt"

func Synthetic(rows int) (Fixture, error) {
	f := Fixture{}
	if rows < 32 || rows > MaxNodes || rows%2 != 0 {
		return f, ErrInvalid
	}
	n := rows / 2
	warehouses := n - 3
	for _, tenant := range []string{"acme", "globex"} {
		for i := 0; i < warehouses; i++ {
			f.Nodes = append(f.Nodes, Node{tenant, fmt.Sprintf("wh-%03d", i), "warehouse", Properties{"name": fmt.Sprintf("%s warehouse %d", tenant, i), "region": []string{"east", "west"}[i%2], "cold": i%3 == 0}})
		}
		for i := 0; i < 3; i++ {
			f.Nodes = append(f.Nodes, Node{tenant, fmt.Sprintf("supplier-%d", i), "supplier", Properties{"name": fmt.Sprintf("%s supplier %d", tenant, i), "region": "east"}})
		}
		add := func(id, from, to, kind string, cost, capacity int64, enabled bool) {
			f.Edges = append(f.Edges, Edge{tenant, id, from, to, kind, Properties{"cost": cost, "capacity": capacity, "enabled": enabled}})
		}
		for i := 0; i < warehouses-1; i++ {
			add(fmt.Sprintf("ring-%03d", i), fmt.Sprintf("wh-%03d", i), fmt.Sprintf("wh-%03d", (i+1)%(warehouses-1)), "route", 10, 100, i != 6)
		}
		add("shortcut-02", "wh-000", "wh-002", "route", 35, 80, tenant == "acme")
		add("parallel-01", "wh-000", "wh-001", "route", 4, 90, true)
		add("shortcut-13", "wh-001", "wh-003", "route", 25, 85, true)
		add("loop-2", "wh-002", "wh-002", "route", 1, 1000, true)
		add("back-30", "wh-003", "wh-000", "route", 9, 100, true)
		add("disabled-47", "wh-004", "wh-007", "route", 1, 100, false)
		for i := 0; i < 3; i++ {
			add(fmt.Sprintf("supplies-%d", i), fmt.Sprintf("supplier-%d", i), fmt.Sprintf("wh-%03d", i*4), "supplies", 2, 75, true)
		}
	}
	return f, Validate(f)
}

type Case struct {
	Name  string `json:"name"`
	Mode  Mode   `json:"mode"`
	Query Query  `json:"query"`
}

func Cases(rows int) []Case {
	isolated := fmt.Sprintf("wh-%03d", rows/2-4)
	return []Case{
		{"forward-reach", Reach, Query{Tenant: "acme", Start: "wh-000", Kind: "route", Hops: 6}},
		{"other-tenant", Reach, Query{Tenant: "globex", Start: "wh-000", Kind: "route", Hops: 4}},
		{"reverse-suppliers", Reach, Query{Tenant: "acme", Start: "wh-003", Kind: "all", Direction: "in", Hops: 6}},
		{"supplier-route", Reach, Query{Tenant: "acme", Start: "supplier-0", Kind: "all", Hops: 4}},
		{"capacity-filter", Reach, Query{Tenant: "acme", Start: "wh-000", Kind: "route", Hops: 6, MinCapacity: 95}},
		{"parallel-diamond-paths", Paths, Query{Tenant: "acme", Start: "wh-000", Target: "wh-003", Kind: "route", Hops: 6}},
		{"reverse-paths", Paths, Query{Tenant: "acme", Start: "wh-003", Target: "supplier-0", Kind: "all", Direction: "in", Hops: 6}},
		{"cheaper-longer-path", Cheapest, Query{Tenant: "acme", Start: "wh-000", Target: "wh-002", Kind: "route", Hops: 6}},
		{"one-hop-bound", Cheapest, Query{Tenant: "acme", Start: "wh-000", Target: "wh-002", Kind: "route", Hops: 1}},
		{"zero-hop", Paths, Query{Tenant: "acme", Start: "wh-002", Target: "wh-002", Kind: "route", Hops: 6}},
		{"disconnected", Paths, Query{Tenant: "acme", Start: "wh-000", Target: isolated, Kind: "all", Hops: 6}},
		{"isolated-start", Reach, Query{Tenant: "acme", Start: isolated, Kind: "all", Hops: 6}},
		{"missing-start", Reach, Query{Tenant: "acme", Start: "missing", Kind: "all", Hops: 6}},
		{"missing-target", Paths, Query{Tenant: "acme", Start: "wh-000", Target: "missing", Kind: "all", Hops: 6}},
		{"missing-tenant", Reach, Query{Tenant: "missing", Start: "wh-000", Kind: "all", Hops: 6}},
		{"full-reachability", Reach, Query{Tenant: "acme", Start: "wh-007", Kind: "route", Hops: MaxReachHops}},
	}
}

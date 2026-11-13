// Package routing is a local unary routing proxy, not a distributed registry.
package routing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // installs client-side Watch health checks
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

const ServiceName = "atlas.inventory.v1.Inventory"

type Registry struct {
	Revision  int64    `json:"revision"`
	Endpoints []string `json:"endpoints"`
}
type Pool struct {
	Conn     *grpc.ClientConn
	RPC      pb.InventoryClient
	resolver *manual.Resolver
	mu       sync.Mutex
	registry Registry
}

func ReadRegistry(path string) (Registry, error) {
	f, e := os.Open(path)
	if e != nil {
		return Registry{}, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 4097))
	if e != nil || len(b) > 4096 {
		return Registry{}, errors.New("registry exceeds 4096 bytes")
	}
	// Require an explicit array: a truncated operator document containing only
	// a revision must not accidentally withdraw every healthy destination.
	var document struct {
		Revision  int64     `json:"revision"`
		Endpoints *[]string `json:"endpoints"`
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if e = d.Decode(&document); e != nil {
		return Registry{}, errors.New("invalid registry JSON")
	}
	if d.Decode(new(any)) != io.EOF {
		return Registry{}, errors.New("trailing registry JSON")
	}
	if document.Endpoints == nil {
		return Registry{}, errors.New("registry requires an explicit endpoints array")
	}
	r := Registry{Revision: document.Revision, Endpoints: *document.Endpoints}
	return r, validate(r)
}
func validate(r Registry) error {
	if r.Revision < 1 || len(r.Endpoints) > 16 {
		return errors.New("invalid registry revision or endpoint count")
	}
	seen := map[string]bool{}
	for _, s := range r.Endpoints {
		host, port, e := net.SplitHostPort(s)
		n, pe := strconv.Atoi(port)
		if e != nil || pe != nil || host == "" || len(host) > 253 || strings.ContainsAny(host, " /\\\t\n\r@%?#") || n < 1 || n > 65535 || seen[s] {
			return errors.New("invalid or duplicate endpoint")
		}
		seen[s] = true
	}
	return nil
}

// NewLocal uses plaintext for the explicitly local Compose/loopback exercise.
// Remote deployments need authenticated registry updates and TLS credentials.
func NewLocal(r Registry) (*Pool, error) {
	if e := validate(r); e != nil {
		return nil, e
	}
	b := manual.NewBuilderWithScheme("atlas-discovery")
	p := &Pool{resolver: b}
	b.InitialState(state(r))
	conn, e := grpc.NewClient("atlas-discovery:///inventory", grpc.WithResolvers(b), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)), grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}],"healthCheckConfig":{"serviceName":"atlas.inventory.v1.Inventory"}}`))
	if e != nil {
		return nil, e
	}
	p.Conn = conn
	p.RPC = pb.NewInventoryClient(conn)
	p.registry = clone(r)
	conn.Connect()
	return p, nil
}
func state(r Registry) resolver.State {
	s := resolver.State{}
	for _, address := range r.Endpoints {
		s.Addresses = append(s.Addresses, resolver.Address{Addr: address})
	}
	return s
}
func clone(r Registry) Registry { r.Endpoints = append([]string(nil), r.Endpoints...); return r }
func (p *Pool) Apply(r Registry) error {
	if e := validate(r); e != nil {
		return e
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if r.Revision < p.registry.Revision || r.Revision == p.registry.Revision && !slices.Equal(r.Endpoints, p.registry.Endpoints) {
		return errors.New("stale or conflicting registry revision")
	}
	if r.Revision == p.registry.Revision {
		return nil
	}
	p.resolver.UpdateState(state(r))
	p.registry = clone(r)
	return nil
}
func (p *Pool) Watch(ctx context.Context, path string, onError func(error)) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	lastError := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r, e := ReadRegistry(path)
			if e == nil {
				e = p.Apply(r)
			}
			if e != nil {
				if e.Error() != lastError && onError != nil {
					onError(e)
				}
				lastError = e.Error()
			} else {
				lastError = ""
			}
		}
	}
}
func (p *Pool) Ready() bool  { return p.Conn.GetState() == connectivity.Ready }
func (p *Pool) Close() error { return p.Conn.Close() }

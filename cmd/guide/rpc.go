package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	rpcclient "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/client"
	pb "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/api/grpc/inventory/v1"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/inventory/ports"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/internal/platform/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func verifyRPCLive(c *client) error {
	target := os.Getenv("ATLAS_RPC_TARGET")
	if target == "" {
		target = "dns:///localhost:18084"
	}
	token := c.keys["acme"]
	if token == "" {
		return errors.New("RPC guide requires configured acme development key")
	}
	rpc, e := rpcclient.New(target, token, insecure.NewCredentials())
	if e != nil {
		return e
	}
	defer rpc.Close()
	id, e := identity.NewID()
	if e != nil {
		return e
	}
	sku := "rpc-" + id
	if _, e = c.expect("POST", "/v1/stock/"+sku+"/receipts", "acme", id+"-seed", map[string]any{"quantity": 10}, 200); e != nil {
		return e
	}
	command := &pb.ReserveRequest{IdempotencyKey: id + "-reserve", Sku: sku, Quantity: 2}
	var out *pb.ReservationReply
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if e = waitRPCReady(ctx, func() (bool, error) {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		reply, err := rpc.Reserve(attempt, command)
		if err != nil {
			if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
				return false, nil
			}
			return false, err
		}
		out = reply
		return true, nil
	}); e != nil {
		return e
	}
	body, e := c.expect("POST", "/v1/reservations", "acme", command.IdempotencyKey, map[string]any{"sku": sku, "quantity": 2}, 201)
	if e != nil {
		return e
	}
	var rest ports.Outcome
	if e = json.Unmarshal(body, &rest); e != nil {
		return e
	}
	if rest.Reservation == nil || rest.Reservation.ID != out.Reservation.Id || rest.Stock.Reserved != 2 {
		return errors.New("live REST/RPC parity failed")
	}
	again, e := rpc.Reserve(ctx, command)
	if e != nil {
		return e
	}
	if !again.Replayed || again.Reservation.Id != out.Reservation.Id {
		return errors.New("live RPC retry duplicated a reservation")
	}
	fmt.Println("Live REST/gRPC share one reservation receipt through the discovered, health-checked routing proxy.")
	return nil
}

func waitRPCReady(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, e := check()
		if e != nil {
			return e
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("RPC readiness did not converge within the caller budget")
		case <-ticker.C:
		}
	}
}

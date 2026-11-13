package actors

import "context"

// Position is trusted adapter metadata, never part of a caller's command or
// actor reference. Copying this value does not inherit the caller's lifetime.
type Position struct {
	Topic     string
	Partition int32
	Offset    int64
}
type positionKey struct{}

func WithPosition(ctx context.Context, p Position) context.Context {
	return context.WithValue(ctx, positionKey{}, p)
}
func PositionFrom(ctx context.Context) Position {
	p, _ := ctx.Value(positionKey{}).(Position)
	return p
}

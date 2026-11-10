// Package operator carries an in-process, tenant-scoped capability created by
// the local operator composition root. Diagnostic metadata grants no access.
package operator

import (
	"context"
	"regexp"

	api "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/contracts/operations"
)

type scopeKey struct{}

var tenant = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

// Local is for a composition root holding the privileged local database binding.
// It is not authentication of a remote user or evidence of production RBAC.
func Local(ctx context.Context, tenantID string) (context.Context, error) {
	if !tenant.MatchString(tenantID) {
		return ctx, api.ErrInvalid
	}
	return context.WithValue(ctx, scopeKey{}, tenantID), nil
}
func Require(ctx context.Context, tenantID string) error {
	value, _ := ctx.Value(scopeKey{}).(string)
	if value == "" || value != tenantID {
		return api.ErrForbidden
	}
	return nil
}

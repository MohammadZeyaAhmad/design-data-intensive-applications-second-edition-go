// Package telemetry records only bounded operational categories. It never
// exports request bodies, credentials, tenant/resource IDs, raw URLs or SQL.
package telemetry

import (
	"errors"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
)

type Options struct {
	Enabled, Durable                           bool
	Service, Version, Address, Token, Endpoint string
	Sample                                     float64
}

var slug = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

func FromEnv(service, version string) (Options, error) {
	o := Options{Service: service, Version: version, Sample: 1, Address: os.Getenv("ATLAS_METRICS_ADDR"), Token: os.Getenv("ATLAS_TELEMETRY_TOKEN"), Endpoint: os.Getenv("ATLAS_OTLP_ENDPOINT")}
	for name, target := range map[string]*bool{"ATLAS_TELEMETRY_ENABLED": &o.Enabled, "ATLAS_DURABLE_TRACE": &o.Durable} {
		if value := os.Getenv(name); value != "" {
			v, err := strconv.ParseBool(value)
			if err != nil {
				return o, errors.New("invalid telemetry flag")
			}
			*target = v
		}
	}
	if value := os.Getenv("ATLAS_TRACE_SAMPLE"); value != "" {
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return o, errors.New("invalid trace sample ratio")
		}
		o.Sample = v
	}
	return o, o.Validate()
}
func (o Options) Validate() error {
	if !slug.MatchString(o.Service) || len(o.Version) > 64 || o.Sample < 0 || o.Sample > 1 || o.Sample != o.Sample {
		return errors.New("invalid telemetry options")
	}
	if o.Durable && (!o.Enabled || o.Endpoint == "") {
		return errors.New("durable tracing requires an enabled exporter")
	}
	if o.Enabled && o.Address != "" {
		host, port, err := net.SplitHostPort(o.Address)
		n, pe := strconv.Atoi(port)
		if err != nil || pe != nil || n < 1 || n > 65535 || len(o.Token) < 32 || (host != "" && host != "0.0.0.0" && host != "127.0.0.1" && host != "localhost" && host != "::1") {
			return errors.New("invalid private metrics listener")
		}
	}
	if o.Endpoint != "" {
		u, err := url.Parse(o.Endpoint)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || (u.Path != "" && u.Path != "/v1/traces") {
			return errors.New("invalid OTLP trace endpoint")
		}
	}
	return nil
}

package encoding

import (
	"bytes"
	"context"
	"io"

	p1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov1"
	p2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/protov2"
	t1 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv1"
	t2 "github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/generated/thriftv2"
	"github.com/MohammadZeyaAhmad/design-data-intensive-applications-second-edition-go/labs/ch05/encoding/model"
	"github.com/apache/thrift/lib/go/thrift"
	"google.golang.org/protobuf/proto"
)

func PreserveProtoRelay(reader int, b []byte, name string) ([]byte, error) {
	if e := wireSize(b); e != nil {
		return nil, e
	}
	if e := scanProto(b); e != nil {
		return nil, e
	}
	if reader == 1 {
		p := &p1.Snapshot{}
		if proto.Unmarshal(b, p) != nil {
			return nil, model.ErrInvalid
		}
		p.CustomerName = name
		return proto.MarshalOptions{Deterministic: true}.Marshal(p)
	}
	if reader == 2 {
		p := &p2.Snapshot{}
		if proto.Unmarshal(b, p) != nil {
			return nil, model.ErrInvalid
		}
		p.DisplayName = name
		return proto.MarshalOptions{Deterministic: true}.Marshal(p)
	}
	return nil, model.ErrInvalid
}

type rpcTransport struct {
	format    string
	request   bytes.Buffer
	response  *bytes.Reader
	processor thrift.TProcessor
	closed    bool
}

func (t *rpcTransport) Open() error  { t.closed = false; return nil }
func (t *rpcTransport) IsOpen() bool { return !t.closed }
func (t *rpcTransport) Close() error { t.closed = true; return nil }
func (t *rpcTransport) Write(b []byte) (int, error) {
	if t.closed || len(b) > model.MaxWire-t.request.Len() {
		return 0, model.ErrBudget
	}
	return t.request.Write(b)
}
func (t *rpcTransport) Read(b []byte) (int, error) {
	if t.response == nil {
		return 0, io.EOF
	}
	return t.response.Read(b)
}
func (t *rpcTransport) RemainingBytes() uint64 {
	if t.response == nil {
		return 0
	}
	return uint64(t.response.Len())
}
func (t *rpcTransport) Flush(ctx context.Context) error {
	b := t.request.Bytes()
	if _, e := scanThrift(ctx, t.format, b, true); e != nil {
		return e
	}
	input, output := thrift.NewTMemoryBufferLen(len(b)), thrift.NewTMemoryBufferLen(1024)
	input.Write(b)
	ok, e := t.processor.Process(ctx, thriftProtocol(t.format, input), thriftProtocol(t.format, output))
	if e != nil {
		return e
	}
	if !ok {
		return model.ErrInvalid
	}
	response := append([]byte(nil), output.Bytes()...)
	if _, e := scanThrift(ctx, t.format, response, true); e != nil {
		return e
	}
	t.response = bytes.NewReader(response)
	t.request.Reset()
	return nil
}

type oldRelay struct{}

func (oldRelay) Relay(_ context.Context, r *t1.Snapshot) (*t1.Snapshot, error) {
	r.CustomerName = "relay:" + r.CustomerName
	return r, nil
}

type newRelay struct{}

func (newRelay) Relay(_ context.Context, r *t2.Snapshot) (*t2.Snapshot, error) {
	r.DisplayName = "relay:" + r.DisplayName
	return r, nil
}

// GeneratedRPC executes real generated clients and processors over a bounded
// in-memory transport. This measures compatibility, not sockets, TLS or retries.
func GeneratedRPC(ctx context.Context, format string, client, server int, r model.Record) (model.Record, error) {
	if format != "thrift-binary" && format != "thrift-compact" {
		return model.Record{}, model.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return model.Record{}, e
	}
	if e := model.Validate(r); e != nil {
		return model.Record{}, e
	}
	var processor thrift.TProcessor
	if server == 1 {
		processor = t1.NewSnapshotGatewayProcessor(oldRelay{})
	} else if server == 2 {
		processor = t2.NewSnapshotGatewayProcessor(newRelay{})
	} else {
		return model.Record{}, model.ErrInvalid
	}
	transport := &rpcTransport{format: format, processor: processor}
	input, output := thriftProtocol(format, transport), thriftProtocol(format, transport)
	result := thrift.NewTMemoryBufferLen(1024)
	if client == 1 {
		c := t1.NewSnapshotGatewayClientProtocol(transport, input, output)
		p, e := c.Relay(ctx, thrift1(r))
		if e != nil {
			return model.Record{}, e
		}
		if e = p.Write(ctx, thriftProtocol(format, result)); e != nil {
			return model.Record{}, e
		}
	} else if client == 2 {
		c := t2.NewSnapshotGatewayClientProtocol(transport, input, output)
		p, e := c.Relay(ctx, thrift2(r))
		if e != nil {
			return model.Record{}, e
		}
		if e = p.Write(ctx, thriftProtocol(format, result)); e != nil {
			return model.Record{}, e
		}
	} else {
		return model.Record{}, model.ErrInvalid
	}
	return (&Suite{}).Decode(ctx, format, client, result.Bytes())
}

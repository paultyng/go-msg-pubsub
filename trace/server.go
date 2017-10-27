package trace

import (
	"context"

	"cloud.google.com/go/trace"
	msg "github.com/zerofox-oss/go-msg"
)

type server struct {
	inner msg.Server
	cli   *trace.Client
}

const receiveSpanName = "go-msg.Server.Receive"

// NewServer returns a go-msg.Server that implements Stackdriver trace.
func NewServer(inner msg.Server, cli *trace.Client) msg.Server {
	return &server{
		inner: inner,
		cli:   cli,
	}
}

type receiver struct {
	inner msg.Receiver
	cli   *trace.Client
}

func (s *server) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

func (s *server) Serve(inner msg.Receiver) error {
	rec := &receiver{
		inner: inner,
		cli:   s.cli,
	}

	return s.inner.Serve(rec)
}

func (r *receiver) Receive(ctx context.Context, m *msg.Message) error {
	span := r.cli.NewSpan(receiveSpanName)
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)
	return r.inner.Receive(ctx, m)
}

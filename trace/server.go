package trace

import (
	"context"

	"cloud.google.com/go/trace"
	msg "github.com/zerofox-oss/go-msg"
)

type server struct {
	inner msg.Server
}

const receiveSpanName = "go-msg.Server.Receive"

// NewServer returns a go-msg.Server that implements Stackdriver trace.
func NewServer(inner msg.Server) msg.Server {
	return &server{
		inner: inner,
	}
}

type receiver struct {
	inner msg.Receiver
}

func (s *server) Shutdown(ctx context.Context) error {
	return s.inner.Shutdown(ctx)
}

func (s *server) Serve(inner msg.Receiver) error {
	rec := &receiver{
		inner: inner,
	}

	return s.inner.Serve(rec)
}

func (r *receiver) Receive(ctx context.Context, m *msg.Message) error {
	span := trace.FromContext(ctx)
	if span != nil {
		child := span.NewChild(receiveSpanName)
		defer child.Finish()

		// TODO: labels for attributes? an ID attribute? callback for labels?

		ctx = trace.NewContext(ctx, child)
	}

	return r.inner.Receive(ctx, m)
}

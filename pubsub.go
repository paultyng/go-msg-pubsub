package pubsub

import (
	"bytes"
	"context"
	"errors"
	"sync"

	gcloud "cloud.google.com/go/pubsub"
	"github.com/zerofox-oss/go-msg"
)

// Client represents a concrete implementation of a pubsub publisher client.
type Client interface {
	Topic(context.Context, string) (msg.Topic, error)
}

type psClient struct {
	cli *gcloud.Client
}

// New returns a new instance of a pubsub Client for a given gcloud project.
func New(ctx context.Context, projectID string) (Client, error) {
	cli, err := gcloud.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	return &psClient{cli: cli}, nil
}

type psTopic struct {
	*gcloud.Topic
}

func (c *psClient) Topic(ctx context.Context, name string) (msg.Topic, error) {
	t := &psTopic{
		Topic: c.cli.Topic(name),
	}

	if exists, err := t.Exists(ctx); err != nil {
		return nil, err
	} else if !exists {
		t.Topic, err = c.cli.CreateTopic(ctx, name)
		if err != nil {
			return nil, err
		}
	}

	return t, nil
}

type psMessage struct {
	*gcloud.Topic
	sync.Mutex

	attributes msg.Attributes
	buf        *bytes.Buffer
	closed     bool
	ctx        context.Context
}

func psAtts(src msg.Attributes) (map[string]string, error) {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		if len(v) > 1 {
			return nil,
				errors.New("only single value attributes allowed in pubsub messages")
		}
		if len(v) > 0 {
			dst[k] = v[0]
		} else {
			dst[k] = ""
		}
	}

	return dst, nil
}

func (t *psTopic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &psMessage{
		Topic:      t.Topic,
		ctx:        ctx,
		attributes: make(map[string][]string),
		buf:        bytes.NewBuffer([]byte{}),
	}
}

func (m *psMessage) Attributes() *msg.Attributes {
	return &m.attributes
}

func (m *psMessage) Write(p []byte) (int, error) {
	m.Lock()
	defer m.Unlock()

	if m.closed {
		return 0, msg.ErrClosedMessageWriter
	}

	return m.buf.Write(p)
}

func (m *psMessage) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.closed {
		return msg.ErrClosedMessageWriter
	}
	m.closed = true

	atts, err := psAtts(m.attributes)
	if err != nil {
		return err
	}

	psm := &gcloud.Message{
		Data:       m.buf.Bytes(),
		Attributes: atts,
	}

	result := m.Publish(m.ctx, psm)
	_, err = result.Get(m.ctx)

	return err
}

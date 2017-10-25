package pubsub

import (
	"bytes"
	"context"
	"sync"
	"time"

	oldcontext "golang.org/x/net/context"

	gcloud "cloud.google.com/go/pubsub"
	msg "github.com/zerofox-oss/go-msg"
)

// MessagePublishTimeFormat is the round trip time format for the publish time
// attribute.
const MessagePublishTimeFormat = time.RFC3339

// These constants contain well-known keys for attributes.
const (
	AttrMessageID          = "_ps_message_id"
	AttrMessagePublishTime = "_ps_message_publishtime"
)

// MessageID returns the pub/sub ID of the original message.
func MessageID(atts msg.Attributes) string {
	vals, ok := atts[AttrMessageID]
	if !ok || len(vals) != 1 {
		return ""
	}

	return vals[0]
}

// MessagePublishTime returns the pub/sub publish time of the original message.
func MessagePublishTime(atts msg.Attributes) time.Time {
	vals, ok := atts[AttrMessagePublishTime]
	if !ok || len(vals) != 1 {
		return time.Time{}
	}

	t, err := time.Parse(MessagePublishTimeFormat, vals[0])
	if err != nil {
		return time.Time{}
	}

	return t
}

var _ msg.Server = &psServer{}

type psServer struct {
	sync.Mutex

	sub    *gcloud.Subscription
	cancel context.CancelFunc
}

// NewServer returns a server implementation for Google's Cloud Pub/Sub.
//
// Serve will only receive messages currently in the subscription and then exit.
// This is currently a limitation in the SDK for Cloud Pub/Sub Pull model. In
// the future Serve may instead iterate and add a sleep to emulate more of a
// Push model or support an HTTPS endpoint to use Pub/Sub's push.
func NewServer(ctx context.Context, projectID, subID, topicID string) (msg.Server, error) {
	cli, err := gcloud.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// ignore returned errors, which will be "already exists". If they're fatal
	// errors, then following calls (e.g. in the subscribe function) will also fail.
	topic, _ := cli.CreateTopic(ctx, topicID)
	subscription, _ := cli.CreateSubscription(ctx, subID, gcloud.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 60 * time.Second,
	})

	return &psServer{
		sub: subscription,
	}, nil
}

func copyAttributes(src map[string]string) msg.Attributes {
	dst := make(msg.Attributes, len(src))

	for k, v := range src {
		dst[k] = []string{v}
	}

	return dst
}

func newAttributes(src *gcloud.Message) msg.Attributes {
	atts := copyAttributes(src.Attributes)

	atts[AttrMessageID] = []string{src.ID}
	atts[AttrMessagePublishTime] = []string{src.PublishTime.Format(MessagePublishTimeFormat)}

	return atts
}

func (s *psServer) Serve(r msg.Receiver) error {
	ctx := context.Background()
	s.Lock()
	ctx, s.cancel = context.WithCancel(ctx)
	s.Unlock()

	return s.sub.Receive(ctx, func(ctx oldcontext.Context, src *gcloud.Message) {
		dst := &msg.Message{
			Attributes: newAttributes(src),
			Body:       bytes.NewReader(src.Data),
		}

		err := r.Receive(ctx, dst)
		if err != nil {
			src.Nack()
			return
		}

		src.Ack()
	})
}

func (s *psServer) Shutdown(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

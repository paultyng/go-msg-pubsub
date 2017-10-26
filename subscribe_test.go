package pubsub

import (
	"fmt"
	"testing"
	"time"

	gcloud "cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/zerofox-oss/go-msg"
)

var (
	halloween17  = time.Date(2017, 10, 31, 20, 0, 0, 0, time.UTC)
	halloween17s = halloween17.Format(MessagePublishTimeFormat)
)

func TestMessageID(t *testing.T) {
	for i, c := range []struct {
		expected string
		atts     msg.Attributes
	}{
		{"", nil},
		{"", msg.Attributes{}},
		{"foo", msg.Attributes{AttrMessageID: []string{"foo"}}},
		{"", msg.Attributes{AttrMessageID: []string{"foo", "bar"}}},
	} {
		t.Run(fmt.Sprintf("%d %s", i, c.expected), func(t *testing.T) {
			actual := MessageID(c.atts)
			assert.Equal(t, c.expected, actual)
		})
	}
}

func TestMessagePublishTime(t *testing.T) {
	zero := time.Time{}

	for i, c := range []struct {
		expected time.Time
		atts     msg.Attributes
	}{
		{zero, nil},
		{zero, msg.Attributes{}},
		{zero, msg.Attributes{AttrMessagePublishTime: []string{"garbage"}}},
		{halloween17, msg.Attributes{AttrMessagePublishTime: []string{halloween17s}}},
		{zero, msg.Attributes{AttrMessagePublishTime: []string{halloween17s, halloween17s}}},
		{zero, msg.Attributes{AttrMessagePublishTime: []string{halloween17s, "garbage"}}},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual := MessagePublishTime(c.atts)
			assert.Equal(t, c.expected, actual)
		})
	}
}

func TestNewAttributes(t *testing.T) {
	for i, c := range []struct {
		expected msg.Attributes
		src      *gcloud.Message
	}{
		{
			msg.Attributes{
				AttrMessageID:          []string{"3"},
				AttrMessagePublishTime: []string{halloween17s},
			},
			&gcloud.Message{
				ID:          "3",
				PublishTime: halloween17,
			},
		},
		{
			msg.Attributes{
				AttrMessageID:          []string{"3"},
				AttrMessagePublishTime: []string{halloween17s},
				"foo":   []string{"bar"},
				"Cap":   []string{"cap"},
				"mIxEd": []string{"mIxEd"},
			},
			&gcloud.Message{
				ID:          "3",
				PublishTime: halloween17,
				Attributes: map[string]string{
					"foo":   "bar",
					"Cap":   "cap",
					"mIxEd": "mIxEd",
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual := newAttributes(c.src)
			assert.Equal(t, c.expected, actual)
		})
	}
}

package redmq

import (
	"fmt"
	"strings"
	"time"
)

// my-topic.meta: {
//   topic:                     *topicMeta // set when topic created, never change
//   topic-idle-since:          int64      // update when add or read or read group
//   group-my-group-idle-since: int64      // update when read group
// }

const (
	topicMetaField             = "topic"
	topicIdleSinceField        = "topic-idle-since"
	groupIdleSinceFieldPattern = "group-%s-idle-since"
)

type (
	// TopicOptions specify the options on creating a topic
	TopicOptions struct {
		// Topic specify the topic identifier
		Topic string `json:"topic"`

		// min interval between shrinks
		MinShrinkInterval int64 `json:"min-shrink-interval"`

		// min idle time before delete a topic
		DeleteIdleTopicAfter int64 `json:"delete-idle-topic-after"`

		// min empty time before delete a group
		DeleteIdleGroupAfter int64 `json:"delete-idle-group-after"`

		// min idle time before delete a consumer
		DeleteIdleConsumerAfter int64 `json:"delete-idle-consumer-after"`
	}

	// topicMeta specify the metadata of a topic and be saved along with topic
	topicMeta struct {
		*TopicOptions
		CreateAt int64 `json:"create-at"`
	}
)

// newTopicMeta creates a topicMeta from TopicOptions
func newTopicMeta(topicOptions *TopicOptions) *topicMeta {
	r := &topicMeta{
		TopicOptions: &(*topicOptions),
		CreateAt:     time.Now().UnixNano() / 1e6,
	}
	if r.MinShrinkInterval <= 0 {
		r.MinShrinkInterval = 1 * 3600 * 1000 // 1h
	}
	if r.DeleteIdleTopicAfter <= 0 {
		r.DeleteIdleTopicAfter = 24 * 3600 * 1000 // 1d
	}
	if r.DeleteIdleGroupAfter <= 0 {
		r.DeleteIdleGroupAfter = 3 * 3600 * 1000 // 3h
	}
	if r.DeleteIdleConsumerAfter <= 0 {
		r.DeleteIdleConsumerAfter = 3 * 3600 * 1000 // 1h
	}
	return r
}

// Mapping topic or topicMeta to redis key
const (
	topicKeyPrefix    = "redmq.topic.{"
	topicKeyPrefixLen = len(topicKeyPrefix)
	topicKeySuffix    = "}"
	topicKeySuffixLen = len(topicKeySuffix)
	topicKeyPattern   = topicKeyPrefix + "%s" + topicKeySuffix

	topicMetaKeyPrefix    = "redmq.topic.{"
	topicMetaKeyPrefixLen = len(topicMetaKeyPrefix)
	topicMetaKeySuffix    = "}.meta"
	topicMetaKeySuffixLen = len(topicMetaKeySuffix)
	topicMetaKeyPattern   = topicMetaKeyPrefix + "%s" + topicMetaKeySuffix

	topicDLQKeyPrefix    = "redmq.topic.{"
	topicDLQKeyPrefixLen = len(topicDLQKeyPrefix)
	topicDLQKeySuffix    = "}.dlq"
	topicDLQKeySuffixLen = len(topicDLQKeySuffix)
	topicDLQKeyPattern   = topicDLQKeyPrefix + "%s" + topicDLQKeySuffix
)

func topicKey(topic string) string {
	if len(topic) == 0 {
		panic("Topic cannot be empty")
	}
	return fmt.Sprintf(topicKeyPattern, topic)
}

func topicMetaKey(topic string) string {
	if len(topic) == 0 {
		panic("Topic cannot be empty")
	}
	return fmt.Sprintf(topicMetaKeyPattern, topic)
}

func topicDLQKey(topic string) string {
	if len(topic) == 0 {
		panic("Topic cannot be empty")
	}
	return fmt.Sprintf(topicDLQKeyPattern, topic)
}

func topicKeys(topics []string) []string {
	keys := make([]string, 0, len(topics))
	for _, topic := range topics {
		keys = append(keys, topicKey(topic))
	}
	return keys
}

func topicMetaKeys(topics []string) []string {
	keys := make([]string, 0, len(topics))
	for _, topic := range topics {
		keys = append(keys, topicMetaKey(topic))
	}
	return keys
}

func topicDLQKeys(topics []string) []string {
	keys := make([]string, 0, len(topics))
	for _, topic := range topics {
		keys = append(keys, topicDLQKey(topic))
	}
	return keys
}

func topicRelatedKeys(topics ...string) []string {
	r := make([]string, 0, len(topics)*3)
	r = append(r, topicKeys(topics)...)
	r = append(r, topicMetaKeys(topics)...)
	r = append(r, topicDLQKeys(topics)...)
	return r
}

func parseTopicKey(s string) (string, bool) {
	n := len(s)
	if n <= topicKeyPrefixLen+topicKeySuffixLen ||
		!strings.HasPrefix(s, topicKeyPrefix) ||
		!strings.HasSuffix(s, topicKeySuffix) {
		return "", false
	}
	return s[topicKeyPrefixLen : n-topicKeySuffixLen], true
}

func parseTopicMetaKey(s string) (string, bool) {
	n := len(s)
	if n <= topicMetaKeyPrefixLen+topicMetaKeySuffixLen ||
		!strings.HasPrefix(s, topicMetaKeyPrefix) ||
		!strings.HasSuffix(s, topicMetaKeySuffix) {
		return "", false
	}
	return s[topicMetaKeyPrefixLen : n-topicMetaKeySuffixLen], true
}

func groupIdleSinceField(s string) string {
	return fmt.Sprintf(groupIdleSinceFieldPattern, s)
}

package redmq

import (
	"fmt"
	"strings"
	"time"
)

// TopicOptions specify the options on creating a topic
type TopicOptions struct {
	// Topic specify the topic identifier
	Topic string `json:"topic,omitempty"`
}

// topicMeta specify the metadata of a topic and be saved along with topic
type topicMeta struct {
	*TopicOptions
	CreateAt time.Time `json:"create_at,omitempty"`
}

// newTopicMeta creates a topicMeta from TopicOptions
func newTopicMeta(topicOptions *TopicOptions) *topicMeta {
	return &topicMeta{
		TopicOptions: topicOptions,
		CreateAt:     time.Now(),
	}
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

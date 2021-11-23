package redmq

import "fmt"

var (
	TopicAlreadyExistsError = fmt.Errorf("Topic already exists")
	TopicNotFoundError      = fmt.Errorf("Topic not found")
)

package kafka

import "errors"

var (
	ErrInvalidPartitionCount = errors.New("cannot reduce number of partitions")
	ErrSamePartitionCount    = errors.New("topic has the same partition count")
	ErrInvalidTopic          = errors.New("topic is invalid")
)

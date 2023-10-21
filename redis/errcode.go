package redis

import "errors"

var (
	ErrNoMsg            = errors.New("no msg received")
	ErrInvalidMsgFormat = errors.New("invalid msg format")
)

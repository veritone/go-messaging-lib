package messaging

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	events "github.com/veritone/core-messages/generated/go/events"
)

func NameFromEvent(payload []byte) string {
	var e events.VtEvent
	err := proto.Unmarshal(payload, &e)
	if err != nil {
		return ""
	}
	eventName, err := ptypes.AnyMessageName(e.Data)
	if err != nil {
		return ""
	}
	return eventName
}

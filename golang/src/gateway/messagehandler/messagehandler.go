package messagehandler

import (
	"sync/atomic"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var globalSessionCounter atomic.Uint64
const stepSessionCounter = 1
const gatewaySessionID = "gateway"

type MessageHandler struct {
	sessionID uint64
}

func NewMessageHandler() MessageHandler {

  sessionID := globalSessionCounter.Add(stepSessionCounter);
	return MessageHandler{
		sessionID: uint64(sessionID),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	body, err := inner.SerializeData(data, messageHandler.sessionID)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: body}, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	body, err := inner.SerializeEOF(messageHandler.sessionID, gatewaySessionID)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: body}, nil
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	msg, err := inner.DeserializeMessage(message.Body)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (messageHandler *MessageHandler) SessionID()  (uint64) {
	return messageHandler.sessionID
}

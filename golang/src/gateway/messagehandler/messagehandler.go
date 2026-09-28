package messagehandler

import (
	"sync/atomic"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var globalSessionCounter atomic.Uint64
const stepSessionCounter = 1

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
	return inner.SerializeMessage(data, messageHandler.sessionID)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(data, messageHandler.sessionID)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	return fruitRecords, nil
}

func (messageHandler *MessageHandler) SessionID()  (uint64) {
	return messageHandler.sessionID
}

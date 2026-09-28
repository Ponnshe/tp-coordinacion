package messagehandler

import (
	"fmt"
	"testing"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TestNewMessageHandlerAssignsUniqueClientID(t *testing.T) {
	t.Cleanup(func() {
		globalSessionCounter.Store(0)
	})
	
	handler1 := NewMessageHandler()
	handler2 := NewMessageHandler()
	
	handler1ID := handler1.SessionID()
	handler2ID := handler2.SessionID()
	
	if handler1ID == handler2ID {
		t.Errorf("The handlers must have different SessionID, but both have: %d", handler1ID)
	}
	
	if handler1ID <= 0 {
		t.Errorf("SessionID must be positive, but it is: %d", handler1ID)
	}

	if handler2ID <= 0 {
		t.Errorf("SessionID must be positive, but it is: %d", handler2ID)
	}
}

func TestSerializeDataMessageReturnSessionID(t *testing.T) {
	t.Cleanup(func() {
		globalSessionCounter.Store(0)
	})

	handler := NewMessageHandler()

	currentID := handler.SessionID()

	fruit := fruititem.FruitItem{
		Fruit: "Manzana", 
		Amount: 5,
	}

	msg, err := handler.SerializeDataMessage(fruit)

	if err != nil{
		t.Fatalf("Error inesperado: %v", err)
	}

	expectedJSON := fmt.Sprintf(`[%d,[["Manzana",5]]]`, currentID)

	if msg.Body != expectedJSON {
		t.Errorf("Esperaba el JSON %s, pero obtuve %s", expectedJSON, msg.Body)
	}
}

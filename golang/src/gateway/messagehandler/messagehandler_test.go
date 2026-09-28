package messagehandler

import (
	"testing"
)

func TestNewMessageHandlerAssignsUniqueClientID(t *testing.T) {
	
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

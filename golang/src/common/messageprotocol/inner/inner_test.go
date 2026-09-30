package inner

import (
	"testing"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TestSerializeDataMessage(t *testing.T) {
	fruits := []fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5 },
	}

	var sessionID uint64 = 123

	jsonString, err := SerializeData(fruits, sessionID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}
	expectedJSON := `["DATA",123,[["Manzana",5]]]`

	if jsonString != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, jsonString)
	}
}

func TestSerializeEOFMessage(t *testing.T) {
	var sessionID uint64 = 123;
	var nodeID = "sum-1";

	jsonString, err := SerializeEOF(sessionID, nodeID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	expectedJSON := `["EOF",123,"sum-1"]`

	if jsonString != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, jsonString)
	}
}

func TestSerializeSynMessage(t *testing.T) {
	var nodeID = "sum-1"

	jsonString, err := SerializeSYN(nodeID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	expectedJSON := `["SYN","sum-1"]`

	if jsonString != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, jsonString)
	}
}

func TestDeserializeDataMessage(t *testing.T) {
	jsonString := `["DATA",123,[["Manzana",5],["Pera",10]]]`
	
	msg, err := DeserializeMessage(jsonString)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	if msg.Type != MsgTypeData {
		t.Errorf("Expected Type %s, got %s", MsgTypeData, msg.Type)
	}
	if msg.SessionID != 123 {
		t.Errorf("Expected SessionID 123, got %d", msg.SessionID)
	}
	if len(msg.Data) != 2 {
		t.Fatalf("Expected 2 Data items, got %d", len(msg.Data))
	}
	if msg.Data[0].Fruit != "Manzana" || msg.Data[0].Amount != 5 {
		t.Errorf("Unexpected first fruit item: %v", msg.Data[0])
	}
	if msg.Data[1].Fruit != "Pera" || msg.Data[1].Amount != 10 {
		t.Errorf("Unexpected second fruit item: %v", msg.Data[1])
	}
}

func TestDeserializeEOFMessage(t *testing.T) {
	jsonString := `["EOF",123,"sum-1"]`
	
	msg, err := DeserializeMessage(jsonString)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	if msg.Type != MsgTypeEOF {
		t.Errorf("Expected Type %s, got %s", MsgTypeEOF, msg.Type)
	}
	if msg.SessionID != 123 {
		t.Errorf("Expected SessionID 123, got %d", msg.SessionID)
	}
	if msg.NodeID != "sum-1" {
		t.Errorf("Expected NodeID sum-1, got %s", msg.NodeID)
	}
}

func TestDeserializeSYNMessage(t *testing.T) {
	jsonString := `["SYN","sum-1"]`
	
	msg, err := DeserializeMessage(jsonString)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	if msg.Type != MsgTypeSYN {
		t.Errorf("Expected Type %s, got %s", MsgTypeSYN, msg.Type)
	}
	if msg.NodeID != "sum-1" {
		t.Errorf("Expected NodeID sum-1, got %s", msg.NodeID)
	}
}

func TestDeserializeInvalidMessage(t *testing.T) {
	jsonString := `["INVALID",123]`
	_, err := DeserializeMessage(jsonString)
	if err == nil {
		t.Fatal("Expected error for invalid message type, got nil")
	}

	jsonStringNotArray := `{"type": "DATA"}`
	_, err = DeserializeMessage(jsonStringNotArray)
	if err == nil {
		t.Fatal("Expected error for non-array message, got nil")
	}
}


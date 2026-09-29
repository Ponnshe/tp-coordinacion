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

	msg, err := SerializeData(fruits, sessionID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}
	expectedJSON := `["DATA",123,[["Manzana",5]]]`

	if msg.Body != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, msg.Body)
	}
}

func TestSerializeEOFMessage(t *testing.T) {
	var sessionID uint64 = 123;
	var nodeID = "sum-1";

	msg, err := SerializeEOF(sessionID, nodeID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	expectedJSON := `["EOF",123,"sum-1"]`

	if msg.Body != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, msg.Body)
	}
}

func TestSerializeSynMessage(t *testing.T) {
	var nodeID = "sum-1"

	msg, err := SerializeSYN(nodeID)
	if err != nil {
		t.Fatalf("Unexpected Error: %v", err)
	}

	expectedJSON := `["SYN","sum-1"]`

	if msg.Body != expectedJSON {
		t.Errorf("Expected JSON %s, but got %s", expectedJSON, msg.Body)
	}
}

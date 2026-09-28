package inner

import (
	"testing"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TestSerializeMessageIncludesSessionID(t *testing.T) {
	fruits := []fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5 },
	}

	var sessionID uint64 = 123

	msg, err := SerializeMessage(fruits, sessionID)
	if err != nil {
		t.Fatalf("Error inesperado: %v", err)
	}

	expectedJSON := `[123,[["Manzana",5]]]`

	if msg.Body != expectedJSON {
		t.Errorf("Se esperaba el JSON %s, pero se obtuvo %s", expectedJSON, msg.Body)
	}
}

package join

import (
	"testing"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MockMiddleware struct {
	SentMessages []middleware.Message
}

func (m *MockMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return nil
}
func (m *MockMiddleware) StopConsuming() error { return nil }
func (m *MockMiddleware) Send(msg middleware.Message, routingKey string) error {
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}
func (m *MockMiddleware) Close() error { return nil }

func createTestJoin() (*Join, *MockMiddleware) {
	outputQueue := &MockMiddleware{}
	j := &Join{
		outputQueue:           outputQueue,
		topSize:               2,
		registeredAggregators: make(map[string]bool),
		sessions:              make(map[uint64]*SessionState),
		eventsChannel:         make(chan middleware.Event, 10),
	}
	return j, outputQueue
}

func TestHandleSYNMessage(t *testing.T) {
	j, _ := createTestJoin()

	synMsg, _ := inner.SerializeSYN("agg_1")
	j.handleMessage(middleware.Event{Message: middleware.Message{Body: synMsg}, Ack: func() {}, Nack: func() {}})

	if !j.registeredAggregators["agg_1"] {
		t.Errorf("Expected agg_1 to be registered")
	}
}

func TestHandleDataMessage(t *testing.T) {
	j, _ := createTestJoin()

	dataMsg, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5},
	}, 123)

	j.handleMessage(middleware.Event{Message: middleware.Message{Body: dataMsg}, Ack: func() {}, Nack: func() {}})

	state, ok := j.sessions[123]
	if !ok {
		t.Fatalf("Expected session 123 to be created")
	}
	if state.FruitItemMap["Manzana"].Amount != 5 {
		t.Errorf("Expected 5 Manzanas, got %d", state.FruitItemMap["Manzana"].Amount)
	}
}

func TestHandleEOFMessage(t *testing.T) {
	j, outQueue := createTestJoin()

	// Register two aggregators
	j.registeredAggregators["agg_1"] = true
	j.registeredAggregators["agg_2"] = true

	// Inject partial tops
	dataMsg1, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 50},
		{Fruit: "Pera", Amount: 10},
	}, 456)
	j.handleMessage(middleware.Event{Message: middleware.Message{Body: dataMsg1}, Ack: func() {}, Nack: func() {}})

	dataMsg2, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Uva", Amount: 60},
		{Fruit: "Manzana", Amount: 5}, // If somehow it repeats, it will sum, giving 55
	}, 456)
	j.handleMessage(middleware.Event{Message: middleware.Message{Body: dataMsg2}, Ack: func() {}, Nack: func() {}})

	// Send EOF from agg_1
	eof1, _ := inner.SerializeEOF(456, "agg_1")
	j.handleMessage(middleware.Event{Message: middleware.Message{Body: eof1}, Ack: func() {}, Nack: func() {}})

	// Should not have sent anything yet
	if len(outQueue.SentMessages) != 0 {
		t.Errorf("Expected 0 messages sent before all EOFs received")
	}

	// Send EOF from agg_2
	eof2, _ := inner.SerializeEOF(456, "agg_2")
	j.handleMessage(middleware.Event{Message: middleware.Message{Body: eof2}, Ack: func() {}, Nack: func() {}})

	// Now it should have sent ONE message (the FINAL DATA TOP)
	if len(outQueue.SentMessages) != 1 {
		t.Fatalf("Expected 1 final message sent, got %d", len(outQueue.SentMessages))
	}

	// Verify session cleanup
	if _, exists := j.sessions[456]; exists {
		t.Errorf("Expected session 456 to be deleted from memory")
	}
}

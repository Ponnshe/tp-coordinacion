package aggregation

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

func createTestAggregation() (*Aggregation, *MockMiddleware) {
	outputQueue := &MockMiddleware{}
	a := &Aggregation{
		nodeID:         "aggregator_1",
		outputQueue:    outputQueue,
		topSize:        2,
		registeredSums: make(map[string]bool),
		sessions:       make(map[uint64]*SessionState),
		eventsChannel:  make(chan InternalEvent, 10),
	}
	return a, outputQueue
}

func TestHandleSYNMessage(t *testing.T) {
	a, _ := createTestAggregation()
	
	synMsg, _ := inner.SerializeSYN("sum_1")
	a.handleMessage(middleware.Event{Message: middleware.Message{Body: synMsg}, Ack: func(){}, Nack: func(){}})

	if !a.registeredSums["sum_1"] {
		t.Errorf("Expected sum_1 to be registered")
	}
}

func TestHandleDataMessage(t *testing.T) {
	a, _ := createTestAggregation()
	
	dataMsg, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5},
	}, 123)
	
	a.handleMessage(middleware.Event{Message: middleware.Message{Body: dataMsg}, Ack: func(){}, Nack: func(){}})
	
	state, ok := a.sessions[123]
	if !ok {
		t.Fatalf("Expected session 123 to be created")
	}
	if state.FruitItemMap["Manzana"].Amount != 5 {
		t.Errorf("Expected 5 Manzanas, got %d", state.FruitItemMap["Manzana"].Amount)
	}
}

func TestHandleEOFMessage(t *testing.T) {
	a, outQueue := createTestAggregation()
	
	// Register two sums
	a.registeredSums["sum_1"] = true
	a.registeredSums["sum_2"] = true
	
	// Inject data for a session
	dataMsg, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5},
		{Fruit: "Pera", Amount: 10},
		{Fruit: "Uva", Amount: 2}, // topSize is 2, so Uva should be dropped
	}, 456)
	a.handleMessage(middleware.Event{Message: middleware.Message{Body: dataMsg}, Ack: func(){}, Nack: func(){}})

	// Send EOF from sum_1
	eof1, _ := inner.SerializeEOF(456, "sum_1")
	a.handleMessage(middleware.Event{Message: middleware.Message{Body: eof1}, Ack: func(){}, Nack: func(){}})
	
	// Should not have sent anything yet
	if len(outQueue.SentMessages) != 0 {
		t.Errorf("Expected 0 messages sent before all EOFs received")
	}
	
	// Send EOF from sum_2
	eof2, _ := inner.SerializeEOF(456, "sum_2")
	a.handleMessage(middleware.Event{Message: middleware.Message{Body: eof2}, Ack: func(){}, Nack: func(){}})
	
	// Now it should have sent TOP data AND EOF
	if len(outQueue.SentMessages) != 2 {
		t.Fatalf("Expected 2 messages sent (TOP + EOF), got %d", len(outQueue.SentMessages))
	}

	// Verify session cleanup
	if _, exists := a.sessions[456]; exists {
		t.Errorf("Expected session 456 to be deleted from memory")
	}
}

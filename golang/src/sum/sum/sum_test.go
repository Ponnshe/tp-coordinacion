package sum

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

func (m *MockMiddleware) StopConsuming() error {
	return nil
}

func (m *MockMiddleware) Send(msg middleware.Message, routingKey string) error {
	m.SentMessages = append(m.SentMessages, msg)
	return nil
}

func (m *MockMiddleware) Close() error {
	return nil
}

func createTestSum() (*Sum, *MockMiddleware, *MockMiddleware) {
	outputExchange := &MockMiddleware{}
	controlExchange := &MockMiddleware{}

	s := &Sum{
		nodeID:            "sum_1",
		aggregationPrefix: "aggregation",
		aggregationAmount: 3,
		outputExchange:    outputExchange,
		controlExchange:   controlExchange,
		sessions:        make(map[uint64]*SessionState),
		finished:        make(map[uint64]bool),
		eventsChannel:   make(chan InternalEvent, 10),
	}
	return s, outputExchange, controlExchange
}

func TestHandleDataMessage(t *testing.T) {
	s, _, _ := createTestSum()

	dataStr, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 5},
		{Fruit: "Pera", Amount: 2},
	}, 123)

	event := InternalEvent{
		Type:    EventFromGateway,
		Message: middleware.Message{Body: dataStr},
		Ack:     func() {},
		Nack:    func() {},
	}

	s.handleMessage(event)

	state, exists := s.sessions[123]
	if !exists {
		t.Fatalf("Session 123 was not created")
	}

	if state.FruitItemMap["Manzana"].Amount != 5 {
		t.Errorf("Expected 5 Manzanas, got %d", state.FruitItemMap["Manzana"].Amount)
	}
	
	if state.FruitItemMap["Pera"].Amount != 2 {
		t.Errorf("Expected 2 Peras, got %d", state.FruitItemMap["Pera"].Amount)
	}

	// Send more data to test accumulation
	dataStr2, _ := inner.SerializeData([]fruititem.FruitItem{
		{Fruit: "Manzana", Amount: 3},
	}, 123)

	event2 := InternalEvent{
		Type:    EventFromGateway,
		Message: middleware.Message{Body: dataStr2},
		Ack:     func() {},
		Nack:    func() {},
	}

	s.handleMessage(event2)
	
	if state.FruitItemMap["Manzana"].Amount != 8 {
		t.Errorf("Expected 8 Manzanas, got %d", state.FruitItemMap["Manzana"].Amount)
	}
}

func TestHandleEOFFromGateway(t *testing.T) {
	s, outEx, ctrlEx := createTestSum()

	s.sessions[123] = &SessionState{
		FruitItemMap: map[string]fruititem.FruitItem{
			"Manzana": {Fruit: "Manzana", Amount: 5},
		},
	}

	eofStr, _ := inner.SerializeEOF(123, "gateway")

	event := InternalEvent{
		Type:    EventFromGateway,
		Message: middleware.Message{Body: eofStr},
		Ack:     func() {},
		Nack:    func() {},
	}

	s.handleMessage(event)

	// It should have sent totals to outEx (1 message for Manzana)
	// It should have sent its own EOF to outEx (1 message)
	if len(outEx.SentMessages) != 2 {
		t.Errorf("Expected 2 messages sent to aggregators, got %d", len(outEx.SentMessages))
	}

	// It should have broadcasted the EOF to ctrlEx (other sums)
	if len(ctrlEx.SentMessages) != 1 {
		t.Errorf("Expected 1 message broadcasted to other sums, got %d", len(ctrlEx.SentMessages))
	}

	// The session should be in finished
	if !s.finished[123] {
		t.Errorf("Expected session 123 to be marked as finished")
	}

	// The session should be deleted from sessions
	if _, ok := s.sessions[123]; ok {
		t.Errorf("Expected session 123 to be deleted from active sessions")
	}
}

func TestHandleEOFFromControl(t *testing.T) {
	s, outEx, ctrlEx := createTestSum()

	s.sessions[456] = &SessionState{
		FruitItemMap: map[string]fruititem.FruitItem{
			"Pera": {Fruit: "Pera", Amount: 10},
		},
	}

	eofStr, _ := inner.SerializeEOF(456, "gateway")

	event := InternalEvent{
		Type:    EventFromControl,
		Message: middleware.Message{Body: eofStr},
		Ack:     func() {},
		Nack:    func() {},
	}

	s.handleMessage(event)

	// It should have sent totals to outEx (1 message for Pera)
	// It should have sent its own EOF to outEx (1 message)
	if len(outEx.SentMessages) != 2 {
		t.Errorf("Expected 2 messages sent to aggregators, got %d", len(outEx.SentMessages))
	}

	// It MUST NOT broadcast the EOF back to ctrlEx!
	if len(ctrlEx.SentMessages) != 0 {
		t.Errorf("Expected 0 messages broadcasted to other sums, got %d", len(ctrlEx.SentMessages))
	}
}

func TestHandleOwnEOFEcho(t *testing.T) {
	s, outEx, ctrlEx := createTestSum()

	// Pretend this node already processed the EOF from gateway
	s.finished[789] = true

	eofStr, _ := inner.SerializeEOF(789, "gateway")

	event := InternalEvent{
		Type:    EventFromControl, // Eco from broadcast
		Message: middleware.Message{Body: eofStr},
		Ack:     func() {},
		Nack:    func() {},
	}

	s.handleMessage(event)

	// It should completely ignore it
	if len(outEx.SentMessages) != 0 {
		t.Errorf("Expected 0 messages sent to aggregators, got %d", len(outEx.SentMessages))
	}

	if len(ctrlEx.SentMessages) != 0 {
		t.Errorf("Expected 0 messages broadcasted, got %d", len(ctrlEx.SentMessages))
	}

	// Memory leak check: It should have removed the session from finished
	if s.finished[789] {
		t.Errorf("Expected session 789 to be deleted from finished map to prevent memory leak")
	}
}

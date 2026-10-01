package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	ID                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type SessionState struct {
	FruitItemMap map[string]fruititem.FruitItem
	ReceivedEOFs map[string]bool
}

type Aggregation struct {
	nodeID         string
	outputQueue    middleware.Middleware
	inputExchange  middleware.Middleware
	topSize        int
	registeredSums map[string]bool
	sessions       map[uint64]*SessionState
	eventsChannel  chan middleware.Event
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.ID)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	nodeID := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.ID)

	return &Aggregation{
		nodeID:         nodeID,
		outputQueue:    outputQueue,
		inputExchange:  inputExchange,
		topSize:        config.TopSize,
		registeredSums: make(map[string]bool),
		sessions:       make(map[uint64]*SessionState),
		eventsChannel:  make(chan middleware.Event, 100),
	}, nil
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, starting graceful shutdown...")
	aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) Run() {
	synMessageStr, err := inner.SerializeSYN(aggregation.nodeID)
	if err != nil {
		slog.Error("Failed to serialize SYN message", "err", err)
	} else {
		if err := aggregation.outputQueue.Send(middleware.Message{Body: synMessageStr}, ""); err != nil {
			slog.Error("Failed to send SYN message to Joiner", "err", err)
		} else {
			slog.Info("SYN message sent to Joiner", "nodeID", aggregation.nodeID)
		}
	}

	go aggregation.handleSignals()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.eventsChannel <- middleware.Event{Message: msg, Ack: ack, Nack: nack}
		})
		if err != nil {
			slog.Error("Error consuming from input exchange", "err", err)
		}
	}()

	go func() {
		wg.Wait()
		close(aggregation.eventsChannel)
	}()

	slog.Info("Aggregation node started processing events", "nodeID", aggregation.nodeID)
	for event := range aggregation.eventsChannel {
		aggregation.handleMessage(event)
	}

	slog.Info("Events channel closed, closing middleware connections...")
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
	slog.Info("Aggregation node shutdown complete.")
}

func (aggregation *Aggregation) handleMessage(event middleware.Event) {
	defer event.Ack()

	msg, err := inner.DeserializeMessage(event.Message.Body)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch msg.Type {
	case inner.MsgTypeSYN:
		aggregation.handleSYNMessage(msg)
	case inner.MsgTypeData:
		aggregation.handleDataMessage(msg)
	case inner.MsgTypeEOF:
		aggregation.handleEOFMessage(msg)
	}
}

func (aggregation *Aggregation) handleSYNMessage(msg *inner.InnerMessage) {
	aggregation.registeredSums[msg.NodeID] = true
	slog.Info("Registered Sum node", "nodeID", msg.NodeID)
}

func (aggregation *Aggregation) handleDataMessage(msg *inner.InnerMessage) {
	state, ok := aggregation.sessions[msg.SessionID]
	if !ok {
		state = &SessionState{
			FruitItemMap: make(map[string]fruititem.FruitItem),
			ReceivedEOFs: make(map[string]bool),
		}
		aggregation.sessions[msg.SessionID] = state
	}

	for _, fruitRecord := range msg.Data {
		if existing, ok := state.FruitItemMap[fruitRecord.Fruit]; ok {
			state.FruitItemMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			state.FruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) handleEOFMessage(msg *inner.InnerMessage) {
	state, ok := aggregation.sessions[msg.SessionID]
	if !ok {
		// Si llega el EOF antes o sin datos, inicializamos igual el estado
		state = &SessionState{
			FruitItemMap: make(map[string]fruititem.FruitItem),
			ReceivedEOFs: make(map[string]bool),
		}
		aggregation.sessions[msg.SessionID] = state
	}
	
	state.ReceivedEOFs[msg.NodeID] = true

	allReceived := true
	for nodeID := range aggregation.registeredSums {
		if !state.ReceivedEOFs[nodeID] {
			allReceived = false
			break
		}
	}

	if allReceived {
		aggregation.handleSessionComplete(msg.SessionID, state)
		delete(aggregation.sessions, msg.SessionID)
	}
}

func (aggregation *Aggregation) handleSessionComplete(sessionID uint64, state *SessionState) {
	slog.Info("All Sum EOFs received, completing session", "sessionID", sessionID)

	fruitTopRecords := aggregation.buildFruitTop(state)
	
	if fruitTopRecords == nil {
		fruitTopRecords = []fruititem.FruitItem{}
	}

	dataMsgStr, err := inner.SerializeData(fruitTopRecords, sessionID)
	if err == nil {
		aggregation.outputQueue.Send(middleware.Message{Body: dataMsgStr}, "")
	} else {
		slog.Error("While serializing top message", "err", err)
	}

	eofMsgStr, err := inner.SerializeEOF(sessionID, aggregation.nodeID)
	if err == nil {
		aggregation.outputQueue.Send(middleware.Message{Body: eofMsgStr}, "")
	} else {
		slog.Error("While serializing EOF message", "err", err)
	}
}

func (aggregation *Aggregation) buildFruitTop(state *SessionState) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(state.FruitItemMap))
	for _, item := range state.FruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	if finalTopSize == 0 {
		return nil
	}
	return fruitItems[:finalTopSize]
}

package join

import (
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

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
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

type Join struct {
	inputQueue            middleware.Middleware
	outputQueue           middleware.Middleware
	topSize               int
	registeredAggregators map[string]bool
	sessions              map[uint64]*SessionState
	eventsChannel         chan middleware.Event
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:            inputQueue,
		outputQueue:           outputQueue,
		topSize:               config.TopSize,
		registeredAggregators: make(map[string]bool),
		sessions:              make(map[uint64]*SessionState),
		eventsChannel:         make(chan middleware.Event, 100),
	}, nil
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, starting graceful shutdown...")
	join.inputQueue.StopConsuming()
}

func (join *Join) Run() {
	go join.handleSignals()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.eventsChannel <- middleware.Event{Message: msg, Ack: ack, Nack: nack}
		})
		if err != nil {
			slog.Error("Error consuming from input queue", "err", err)
		}
	}()

	go func() {
		wg.Wait()
		close(join.eventsChannel)
	}()

	slog.Info("Join node started processing events")
	for event := range join.eventsChannel {
		join.handleMessage(event)
	}

	slog.Info("Events channel closed, closing middleware connections...")
	join.inputQueue.Close()
	join.outputQueue.Close()
	slog.Info("Join node shutdown complete.")
}

func (join *Join) handleMessage(event middleware.Event) {
	defer event.Ack()

	msg, err := inner.DeserializeMessage(event.Message.Body)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch msg.Type {
	case inner.MsgTypeSYN:
		join.handleSYNMessage(msg)
	case inner.MsgTypeData:
		join.handleDataMessage(msg)
	case inner.MsgTypeEOF:
		join.handleEOFMessage(msg)
	}
}

func (join *Join) handleSYNMessage(msg *inner.InnerMessage) {
	join.registeredAggregators[msg.NodeID] = true
	slog.Info("Registered Aggregator node", "nodeID", msg.NodeID)
}

func (join *Join) handleDataMessage(msg *inner.InnerMessage) {
	state, ok := join.sessions[msg.SessionID]
	if !ok {
		state = &SessionState{
			FruitItemMap: make(map[string]fruititem.FruitItem),
			ReceivedEOFs: make(map[string]bool),
		}
		join.sessions[msg.SessionID] = state
	}

	for _, fruitRecord := range msg.Data {
		if existing, ok := state.FruitItemMap[fruitRecord.Fruit]; ok {
			state.FruitItemMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			state.FruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) handleEOFMessage(msg *inner.InnerMessage) {
	state, ok := join.sessions[msg.SessionID]
	if !ok {
		state = &SessionState{
			FruitItemMap: make(map[string]fruititem.FruitItem),
			ReceivedEOFs: make(map[string]bool),
		}
		join.sessions[msg.SessionID] = state
	}

	state.ReceivedEOFs[msg.NodeID] = true

	allReceived := true
	for nodeID := range join.registeredAggregators {
		if !state.ReceivedEOFs[nodeID] {
			allReceived = false
			break
		}
	}

	if allReceived {
		join.handleSessionComplete(msg.SessionID, state)
		delete(join.sessions, msg.SessionID)
	}
}

func (join *Join) handleSessionComplete(sessionID uint64, state *SessionState) {
	slog.Info("All Aggregator EOFs received, completing session", "sessionID", sessionID)

	fruitTopRecords := join.buildFruitTop(state)

	if fruitTopRecords == nil {
		fruitTopRecords = []fruititem.FruitItem{}
	}

	dataMsgStr, err := inner.SerializeData(fruitTopRecords, sessionID)
	if err == nil {
		if err := join.outputQueue.Send(middleware.Message{Body: dataMsgStr}, ""); err != nil {
			slog.Error("Failed to send top to gateway", "err", err)
		} else {
			slog.Info("Successfully sent final top to Gateway", "sessionID", sessionID)
		}
	} else {
		slog.Error("While serializing top message", "err", err)
	}
}

func (join *Join) buildFruitTop(state *SessionState) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(state.FruitItemMap))
	for _, item := range state.FruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	if finalTopSize == 0 {
		return nil
	}
	return fruitItems[:finalTopSize]
}

package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func (sum *Sum) getAggregationRoutingKey(fruit string) string {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	idx := int(h.Sum32()) % sum.aggregationAmount
	return fmt.Sprintf("%s_%d", sum.aggregationPrefix, idx)
}

type SumConfig struct {
	ID                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type EventType int

const (
	EventFromGateway EventType = iota
	EventFromControl
)

type InternalEvent struct {
	Type    EventType
	Message middleware.Message
	Ack     func()
	Nack    func()
}

type SessionState struct {
	FruitItemMap map[string]fruititem.FruitItem
}

type Sum struct {
	nodeID            string
	aggregationPrefix string
	aggregationAmount int
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	controlExchange   middleware.Middleware
	sessions        map[uint64]*SessionState
	finished        map[uint64]bool
	eventsChannel   chan InternalEvent
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, []string{config.SumPrefix}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	nodeID := fmt.Sprintf("%s_%d", config.SumPrefix, config.ID)

	return &Sum{
		nodeID:            nodeID,
		aggregationPrefix: config.AggregationPrefix,
		aggregationAmount: config.AggregationAmount,
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		controlExchange: controlExchange,
		sessions:        make(map[uint64]*SessionState),
		finished:        make(map[uint64]bool),
		eventsChannel:   make(chan InternalEvent, 100),
	}, nil
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM received, starting graceful shutdown...")
	sum.inputQueue.StopConsuming()
	sum.controlExchange.StopConsuming()
}

func (sum *Sum) Run() {
	synMessageStr, err := inner.SerializeSYN(sum.nodeID)
	if err != nil {
		slog.Error("Failed to serialize SYN message", "err", err)
	} else {
		if err := sum.outputExchange.Send(middleware.Message{Body: synMessageStr}, ""); err != nil {
			slog.Error("Failed to send SYN message to aggregators", "err", err)
		} else {
			slog.Info("SYN message sent to aggregators", "nodeID", sum.nodeID)
		}
	}

	go sum.handleSignals()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.eventsChannel <- InternalEvent{Type: EventFromGateway, Message: msg, Ack: ack, Nack: nack}
		})
		if err != nil {
			slog.Error("Error consuming from input queue", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.eventsChannel <- InternalEvent{Type: EventFromControl, Message: msg, Ack: ack, Nack: nack}
		})
		if err != nil {
			slog.Error("Error consuming from control exchange", "err", err)
		}
	}()

	go func() {
		wg.Wait()
		close(sum.eventsChannel)
	}()

	slog.Info("Sum node started processing events", "nodeID", sum.nodeID)
	for event := range sum.eventsChannel {
		sum.handleMessage(event)
	}

	slog.Info("Events channel closed, closing middleware connections...")
	sum.inputQueue.Close()
	sum.controlExchange.Close()
	sum.outputExchange.Close()
	slog.Info("Sum node shutdown complete.")
}

func (sum *Sum) handleMessage(event InternalEvent) {
	defer event.Ack()

	msg, err := inner.DeserializeMessage(event.Message.Body)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch msg.Type {
	case inner.MsgTypeData:
		state, ok := sum.sessions[msg.SessionID]
		if !ok {
			state = &SessionState{FruitItemMap: make(map[string]fruititem.FruitItem)}
			sum.sessions[msg.SessionID] = state
		}
		
		for _, fruitRecord := range msg.Data {
			if existing, ok := state.FruitItemMap[fruitRecord.Fruit]; ok {
				state.FruitItemMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
			} else {
				state.FruitItemMap[fruitRecord.Fruit] = fruitRecord
			}
		}

	case inner.MsgTypeEOF:
		if event.Type == EventFromControl {
			if sum.finished[msg.SessionID] {
				// It's an echo of our own broadcast. Ignore it and clean up memory.
				delete(sum.finished, msg.SessionID)
				return
			}
		} else if event.Type == EventFromGateway {
			// Mark as finished so we can ignore our own echo later
			sum.finished[msg.SessionID] = true
			
			// Broadcast the EXACT same EOF message to our sibling Sum nodes
			if err := sum.controlExchange.Send(middleware.Message{Body: event.Message.Body}, ""); err != nil {
				slog.Error("Failed to broadcast EOF to control exchange", "err", err)
			}
		}

		// Send totals to aggregators
		state, ok := sum.sessions[msg.SessionID]
		if ok {
			for _, fruitRecord := range state.FruitItemMap {
				dataMsgStr, err := inner.SerializeData([]fruititem.FruitItem{fruitRecord}, msg.SessionID)
				if err == nil {
					rk := sum.getAggregationRoutingKey(fruitRecord.Fruit)
					sum.outputExchange.Send(middleware.Message{Body: dataMsgStr}, rk)
				}
			}
			delete(sum.sessions, msg.SessionID)
		}

		// Send our own EOF to aggregators
		eofMsgStr, err := inner.SerializeEOF(msg.SessionID, sum.nodeID)
		if err == nil {
			sum.outputExchange.Send(middleware.Message{Body: eofMsgStr}, "")
		}

	case inner.MsgTypeSYN:
		// Sums should not receive SYN messages from each other
		slog.Debug("Received SYN message unexpectedly")
	}
}

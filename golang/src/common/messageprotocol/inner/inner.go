package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

const (
	MsgTypeData = "DATA"
	MsgTypeEOF  = "EOF"
	MsgTypeSYN  = "SYN"
)

func serializeJSON(message []any) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJSON(message []byte) ([]any, error) {
	var data []any
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeData(fruitRecords []fruititem.FruitItem, sessionID uint64) (string, error) {
	data := []any{}
	for _, fruitRecord := range fruitRecords {
		datum := []any{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	finalPayload := []any{MsgTypeData,sessionID,data}

	body, err := serializeJSON(finalPayload)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func SerializeSYN(nodeID string) (string, error) {
	finalPayload := []any{MsgTypeSYN,nodeID}

	body, err := serializeJSON(finalPayload)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func SerializeEOF(sessionID uint64, nodeID string) (string, error) {
	finalPayload := []any{MsgTypeEOF,sessionID,nodeID}

	body, err := serializeJSON(finalPayload)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

type InnerMessage struct {
	Type      string
	SessionID uint64
	NodeID    string
	Data      []fruititem.FruitItem
}

func DeserializeMessage(message string) (*InnerMessage, error) {
	data, err := deserializeJSON([]byte(message))
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, errors.New("message is empty")
	}

	msgType, ok := data[0].(string)
	if !ok {
		return nil, errors.New("message type is not a string")
	}

	switch msgType {
	case MsgTypeData:
		return deserializeDataPayload(data)
	case MsgTypeEOF:
		return deserializeEOFPayload(data)
	case MsgTypeSYN:
		return deserializeSYNPayload(data)
	default:
		return nil, errors.New("unknown message type")
	}
}

func deserializeDataPayload(data []any) (*InnerMessage, error) {
	if len(data) < 3 {
		return nil, errors.New("invalid DATA message length")
	}
	
	sessionIDFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("invalid sessionID in DATA message")
	}

	fruitDataArray, ok := data[2].([]any)
	if !ok {
		return nil, errors.New("invalid data payload in DATA message")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range fruitDataArray {
		fruitPair, ok := datum.([]any)
		if !ok || len(fruitPair) < 2 {
			return nil, errors.New("datum is not a valid array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("datum amount is not a number")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return &InnerMessage{
		Type:      MsgTypeData,
		SessionID: uint64(sessionIDFloat),
		Data:      fruitRecords,
	}, nil
}

func deserializeEOFPayload(data []any) (*InnerMessage, error) {
	if len(data) < 3 {
		return nil, errors.New("invalid EOF message length")
	}

	sessionIDFloat, ok := data[1].(float64)
	if !ok {
		return nil, errors.New("invalid sessionID in EOF message")
	}

	nodeID, ok := data[2].(string)
	if !ok {
		return nil, errors.New("invalid nodeID in EOF message")
	}

	return &InnerMessage{
		Type:      MsgTypeEOF,
		SessionID: uint64(sessionIDFloat),
		NodeID:    nodeID,
	}, nil
}

func deserializeSYNPayload(data []any) (*InnerMessage, error) {
	if len(data) < 2 {
		return nil, errors.New("invalid SYN message length")
	}

	nodeID, ok := data[1].(string)
	if !ok {
		return nil, errors.New("invalid nodeID in SYN message")
	}

	return &InnerMessage{
		Type:   MsgTypeSYN,
		NodeID: nodeID,
	}, nil
}

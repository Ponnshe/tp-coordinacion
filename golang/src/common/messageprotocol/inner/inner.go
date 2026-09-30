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

func DeserializeMessage(message string) ([]fruititem.FruitItem, bool, error) {
	data, err := deserializeJSON([]byte(message))
	if err != nil {
		return nil, false, err
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range data {
		fruitPair, ok := datum.([]any)
		if !ok {
			return nil, false, errors.New("datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, false, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, false, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return fruitRecords, len(fruitRecords) == 0, nil
}

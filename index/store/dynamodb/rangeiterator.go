package dynamodb

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type RangeIterator struct {
	store *Store
	start []byte
	end   []byte
	err   error
	valid bool

	lastEvaluatedKey map[string]*dynamodb.AttributeValue
	index            int
	items            []map[string]*dynamodb.AttributeValue

	key []byte
	val []byte
}

func (i *RangeIterator) Seek(k []byte) {
	start := i.start
	if start == nil || (k != nil && bytes.Compare(k, start) > 0) {
		// We're starting a new set of results, so wipe out the paging.
		start = k
		i.lastEvaluatedKey = nil
	}
	qi := &dynamodb.QueryInput{
		TableName:              &i.store.tableName,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("#pk = :pk"),
		ExpressionAttributeNames: map[string]*string{
			"#pk": aws.String("pk"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": partitionKeyAttributeValue(i.store.partition),
		},
		ExclusiveStartKey: i.lastEvaluatedKey,
	}
	if start != nil {
		qi.KeyConditionExpression = aws.String("#pk = :pk AND #sk >= :start")
		qi.ExpressionAttributeNames["#sk"] = aws.String("sk")
		qi.ExpressionAttributeValues[":start"] = &dynamodb.AttributeValue{B: start}
	}
	if start != nil && i.end != nil {
		if bytes.Compare(start, i.end) > 0 {
			// The start is after the end, so we shouldn't return any results.
			i.valid = false
			return
		}
		qi.KeyConditionExpression = aws.String("#pk = :pk AND #sk BETWEEN :start AND :end")
		qi.ExpressionAttributeNames["#sk"] = aws.String("sk")
		qi.ExpressionAttributeValues[":start"] = &dynamodb.AttributeValue{B: start}
		qi.ExpressionAttributeValues[":end"] = &dynamodb.AttributeValue{B: i.end}
		// Filter out the finishing match, because Bleve doesn't expect an inclusive set of results.
		qi.ExpressionAttributeNames["#k"] = aws.String("k")
		qi.FilterExpression = aws.String("#k < :end")
	}
	qo, err := i.store.db.Query(qi)
	if err != nil {
		j, _ := json.MarshalIndent(qi, "", "  ")
		fmt.Println(string(j))
		i.err = err
		i.valid = false
		return
	}
	i.lastEvaluatedKey = qo.LastEvaluatedKey
	i.items = qo.Items
	i.index = 0
	i.valid = len(qo.Items) > 0
	if i.valid {
		item := i.items[i.index]
		i.key = item["sk"].B
		i.val, _ = recordToValue(item)
		i.index++
	}
}

func (i *RangeIterator) Next() {
	// There's no more items in the current page.
	if i.index > len(i.items)-1 {
		// Grab another page if there is one.
		if !isKeyNullOrEmpty(i.lastEvaluatedKey) {
			i.Seek(nil)
			return
		}
		// If there isn't, we're finished.
		i.valid = false
		return
	}
	item := i.items[i.index]
	i.key = item["sk"].B
	i.val, _ = recordToValue(item)
	//TODO: Handle the error.
	i.index++
}

func (i *RangeIterator) Current() ([]byte, []byte, bool) {
	return i.key, i.val, i.valid
}

func (i *RangeIterator) Key() []byte {
	return i.key
}

func (i *RangeIterator) Value() []byte {
	return i.val
}

func (i *RangeIterator) Valid() bool {
	return i.valid
}

func (i *RangeIterator) Close() error {
	return i.err
}

package dynamodb

import (
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
	//TODO: Read forward until k is found, starting from the start key?
	//TODO: I wasn't sure whether it's required to seek if we've got a range iterator?
	qo, err := i.store.db.Query(&dynamodb.QueryInput{
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk >= :start AND sk < :end"),
		ExpressionAttributeNames: map[string]*string{
			"pk": aws.String("pk"),
			"sk": aws.String("sk"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":    partitionKeyAttributeValue(i.store.partition),
			":start": &dynamodb.AttributeValue{B: i.start},
			":end":   &dynamodb.AttributeValue{B: i.end},
		},
		ExclusiveStartKey: i.lastEvaluatedKey,
	})
	if err != nil {
		i.err = err
		i.valid = false
		return
	}
	i.lastEvaluatedKey = qo.LastEvaluatedKey
	i.items = qo.Items
	i.index = 0
	i.valid = len(qo.Items) > 0
}

func (i *RangeIterator) Next() {
	// There's no more items in the current page.
	if i.index >= len(i.items) {
		// If there's more pages to grab.
		if !isKeyNullOrEmpty(i.lastEvaluatedKey) {
			// Grab another page.
			i.Seek(nil)
			// If there's still no results, quit.
			if i.index >= len(i.items) {
				return
			}
		}
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
	return nil
}

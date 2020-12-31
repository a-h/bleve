package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func isKeyNullOrEmpty(lastEvaluatedKey map[string]*dynamodb.AttributeValue) bool {
	return lastEvaluatedKey == nil || len(lastEvaluatedKey) == 0
}

type PrefixIterator struct {
	store  *Store
	prefix []byte
	err    error
	valid  bool

	lastEvaluatedKey map[string]*dynamodb.AttributeValue
	index            int
	items            []map[string]*dynamodb.AttributeValue

	key []byte
	val []byte
}

func (i *PrefixIterator) Seek(k []byte) {
	//TODO: I wasn't sure whether it's required to seek if we've got a prefix iterator?
	qo, err := i.store.db.Query(&dynamodb.QueryInput{
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeNames: map[string]*string{
			"pk": aws.String("pk"),
			"sk": aws.String("sk"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":     partitionKeyAttributeValue(i.store.partition),
			":prefix": &dynamodb.AttributeValue{B: i.prefix},
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

func (i *PrefixIterator) Next() {
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

func (i *PrefixIterator) Current() ([]byte, []byte, bool) {
	return i.key, i.val, i.valid
}

func (i *PrefixIterator) Key() []byte {
	return i.key
}

func (i *PrefixIterator) Value() []byte {
	return i.val
}

func (i *PrefixIterator) Valid() bool {
	return i.valid
}

func (i *PrefixIterator) Close() error {
	return nil
}

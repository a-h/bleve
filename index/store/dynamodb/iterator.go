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
	qi := &dynamodb.QueryInput{
		TableName:              &i.store.tableName,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]*string{
			"#pk": aws.String("pk"),
			"#sk": aws.String("sk"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":     partitionKeyAttributeValue(i.store.partition),
			":prefix": {B: i.prefix},
		},
		ExclusiveStartKey: i.lastEvaluatedKey,
	}
	if k != nil {
		qi.KeyConditionExpression = aws.String("#pk = :pk AND #sk >= :k")
		qi.ExpressionAttributeNames["#k"] = aws.String("k")
		qi.ExpressionAttributeValues[":k"] = &dynamodb.AttributeValue{B: k}
		qi.FilterExpression = aws.String("begins_with(#k, :prefix)")
	}
	qo, err := i.store.db.Query(qi)
	if err != nil {
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

func (i *PrefixIterator) Next() {
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
	return i.err
}

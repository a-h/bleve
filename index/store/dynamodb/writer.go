package dynamodb

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	store *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	return NewDynamoDBBatch(w.store.tableName, w.store.partition)
}

func NewDynamoDBBatch(tableName, partition string) store.KVBatch {
	return &DynamoDBBatch{
		tableName: tableName,
		partition: partition,
		tx:        []*dynamodb.TransactWriteItem{},
	}
}

type DynamoDBBatch struct {
	tableName string
	partition string
	tx        []*dynamodb.TransactWriteItem
}

func partitionKeyAttributeValue(partition string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{S: aws.String(partition)}
}

func sortKeyAttributeValue(key []byte) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{B: key}
}

func valueAttributeByteArrayValue(value []byte) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{B: value}
}

func valueAttributeNumberValue(value []byte) *dynamodb.AttributeValue {
	vv := int64(binary.LittleEndian.Uint64(value))
	return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(vv, 10))}
}

func (batch *DynamoDBBatch) Set(key, value []byte) {
	item := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName: aws.String(batch.tableName),
			Item: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(key),
				"v":  valueAttributeByteArrayValue(value),
			},
		},
	}
	if key[0] == 't' {
		// It must be a term frequency row, replace it with a number so that the database
		// can carry out merge operations.
		item.Put.Item["v"] = valueAttributeNumberValue(value)
	}

	batch.tx = append(batch.tx, item)
}

func (batch *DynamoDBBatch) Delete(key []byte) {
	batch.tx = append(batch.tx, &dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			TableName: aws.String(batch.tableName),
			Key: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(key),
			},
		},
	})
}

func (batch *DynamoDBBatch) Reset() {
	batch.tx = batch.tx[0:]
}

func (batch *DynamoDBBatch) Close() error {
	return nil
}

func (batch *DynamoDBBatch) Merge(key, val []byte) {
	//TODO: It seems that there's only one implementation of merge at the moment, the upsideDownMerge at index/upsidedown/row_merge.go, so this merge is going to replicate that behaviour in DynamoDB using the set operation. This means peeking at the row type when writing to write an N item instead of a B item.
	//TODO: I expect two operations can't affect the same row, so I'll have to work that out.
	vv := int64(binary.LittleEndian.Uint64(val))
	batch.tx = append(batch.tx, &dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			TableName:        aws.String(batch.tableName),
			UpdateExpression: aws.String("add #v :vv"),
			ExpressionAttributeNames: map[string]*string{
				"#v": aws.String("v"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":vv": {N: aws.String(strconv.FormatInt(vv, 10))},
			},
			Key: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(key),
			},
		},
	})
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) (err error) {
	dynamoDBBatch, ok := batch.(*DynamoDBBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch, expected DynamoDBBatch, got %t", batch)
	}
	_, err = w.store.db.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: dynamoDBBatch.tx,
	})
	return
}

func (w *Writer) Close() error {
	return nil
}

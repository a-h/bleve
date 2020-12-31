package dynamodb

import (
	"encoding/base64"
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
		tx:        map[string]*dynamodb.TransactWriteItem{},
	}
}

type DynamoDBBatch struct {
	tableName string
	partition string
	tx        map[string]*dynamodb.TransactWriteItem
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

func (batch *DynamoDBBatch) setTx(key []byte, item *dynamodb.TransactWriteItem) {
	k := base64.RawStdEncoding.EncodeToString(key)
	batch.tx[k] = item
}

func (batch *DynamoDBBatch) Set(key, val []byte) {
	ck := make([]byte, len(key))
	copy(ck, key)
	cv := make([]byte, len(val))
	copy(cv, val)

	item := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName: aws.String(batch.tableName),
			Item: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(ck),
				"k":  sortKeyAttributeValue(ck), // Duplicate of sk to allow filter expression in the PrefixIterator.
				"v":  valueAttributeByteArrayValue(cv),
			},
		},
	}
	if ck[0] == 't' {
		// It must be a term frequency row, replace it with a number so that the database
		// can carry out merge operations.
		item.Put.Item["v"] = valueAttributeNumberValue(cv)
	}
	batch.setTx(ck, item)
}

func (batch *DynamoDBBatch) Delete(key []byte) {
	ck := make([]byte, len(key))
	copy(ck, key)

	item := &dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			TableName: aws.String(batch.tableName),
			Key: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(ck),
			},
		},
	}
	batch.setTx(ck, item)
}

func (batch *DynamoDBBatch) Reset() {
	batch.tx = map[string]*dynamodb.TransactWriteItem{}
}

func (batch *DynamoDBBatch) Close() error {
	return nil
}

func (batch *DynamoDBBatch) Merge(key, val []byte) {
	ck := make([]byte, len(key))
	copy(ck, key)
	cv := make([]byte, len(val))
	copy(cv, val)
	//TODO: It seems that there's only one implementation of merge at the moment, the upsideDownMerge at index/upsidedown/row_merge.go, so this merge is going to replicate that behaviour in DynamoDB using the set operation. This means peeking at the row type when writing to write an N item instead of a B item.
	vv := int64(binary.LittleEndian.Uint64(cv))
	if item, hasExisting := batch.tx[base64.RawStdEncoding.EncodeToString(ck)]; hasExisting {
		//TODO: How should we handle the case that the number can't be parsed? The only place to return an error is on close.
		existingValue, _ := strconv.ParseInt(*item.Update.ExpressionAttributeValues[":vv"].N, 10, 64)
		item.Update.ExpressionAttributeValues[":vv"].N = aws.String(strconv.FormatInt(existingValue+vv, 10))
		return
	}
	item := &dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			TableName:        aws.String(batch.tableName),
			UpdateExpression: aws.String("ADD #v :vv SET #k = :k"),
			ExpressionAttributeNames: map[string]*string{
				"#v": aws.String("v"),
				"#k": aws.String("k"),
			},
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":vv": {N: aws.String(strconv.FormatInt(vv, 10))},
				":k":  sortKeyAttributeValue(ck),
			},
			Key: map[string]*dynamodb.AttributeValue{
				"pk": partitionKeyAttributeValue(batch.partition),
				"sk": sortKeyAttributeValue(ck),
			},
		},
	}
	batch.setTx(ck, item)
}

func (batch *DynamoDBBatch) Items() []*dynamodb.TransactWriteItem {
	items := make([]*dynamodb.TransactWriteItem, len(batch.tx))
	var i int
	for _, v := range batch.tx {
		items[i] = v
		i++
	}
	return items
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
		TransactItems: dynamoDBBatch.Items(),
	})
	return
}

func (w *Writer) Close() error {
	return nil
}

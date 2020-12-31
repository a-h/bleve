package dynamodb

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/blevesearch/bleve/index/store"
)

type Reader struct {
	store *Store
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	output, err := r.store.db.GetItem(&dynamodb.GetItemInput{TableName: &r.store.tableName, ConsistentRead: aws.Bool(true), Key: map[string]*dynamodb.AttributeValue{
		"pk": partitionKeyAttributeValue(r.store.partition),
		"sk": sortKeyAttributeValue(key),
	}})
	if err != nil {
		return nil, err
	}
	return recordToValue(output.Item)
}

func recordToValue(record map[string]*dynamodb.AttributeValue) ([]byte, error) {
	if record["v"].B != nil {
		return record["v"].B, nil
	}
	if record["v"].N != nil {
		v := make([]byte, 8)
		vv, err := strconv.ParseUint(*record["v"].N, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse 'v' of %q", *record["v"].N)
		}
		binary.LittleEndian.PutUint64(v, uint64(vv))
		return v, nil
	}
	return nil, fmt.Errorf("expected record with 'v' attribute containing a byte array or number, got %+v", record)
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	//TODO: Replace this with BatchGetItem
	return store.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := &PrefixIterator{
		store:  r.store,
		prefix: prefix,
	}
	rv.Seek(nil)
	return rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := &RangeIterator{
		store: r.store,
		start: start,
		end:   end,
	}
	rv.Seek(nil)
	return rv
}

func (r *Reader) Close() error {
	return nil
}

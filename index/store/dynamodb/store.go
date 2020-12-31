package dynamodb

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
)

const (
	Name = "dynamodb"
)

type Store struct {
	tableName string
	partition string
	db        *dynamodb.DynamoDB
	mo        store.MergeOperator
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	partition, ok := config["partition"].(string)
	if !ok {
		partition = "bleve"
	}
	tableName, ok := config["tableName"].(string)
	if !ok {
		return nil, errors.New("missing tableName in config")
	}
	region, ok := config["region"].(string)
	if !ok {
		return nil, errors.New("missing region in config")
	}
	db := dynamodb.New(session.New(aws.NewConfig().WithRegion(region)))

	// Set the endpoint, if provided.
	endpoint, ok := config["endpoint"].(string)
	if endpoint != "" {
		db.Endpoint = endpoint
	}

	rv := Store{
		tableName: tableName,
		partition: partition,
		db:        db,
	}
	return &rv, nil
}

func (bs *Store) Close() error {
	return nil
}

func (bs *Store) Reader() (store.KVReader, error) {
	return &Reader{
		store: bs,
	}, nil
}

func (bs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: bs,
	}, nil
}

func (bs *Store) Stats() json.Marshaler {
	return &stats{
		store: bs,
	}
}

func init() {
	registry.RegisterKVStore(Name, New)
}

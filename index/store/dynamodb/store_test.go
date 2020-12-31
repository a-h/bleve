package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
	"github.com/google/uuid"
)

const region = "eu-west-1"

func createLocalTable(t *testing.T) (name string) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		t.Fatalf("failed to create test db session: %v", err)
		return
	}
	name = uuid.New().String()
	client := dynamodb.New(sess)
	client.Endpoint = "http://localhost:8000"
	_, err = client.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: aws.String("B"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       aws.String("RANGE"),
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
		TableName:   aws.String(name),
	})
	if err != nil {
		t.Fatalf("failed to create local table: %v", err)
	}
	return
}

func deleteLocalTable(t *testing.T, name string) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return
	}
	client := dynamodb.New(sess)
	client.Endpoint = "http://localhost:8000"
	_, err = client.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("failed to delete table: %v", err)
	}
}

func open(t *testing.T, mo store.MergeOperator) (rv store.KVStore, tableName string) {
	tableName = createLocalTable(t)
	rv, err := New(mo, map[string]interface{}{"tableName": tableName, "region": region, "endpoint": "http://localhost:8000"})
	if err != nil {
		t.Fatal(err)
	}
	return rv, tableName
}

func cleanup(t *testing.T, s store.KVStore, tableName string) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
	deleteLocalTable(t, tableName)
}

func TestDynamoDBKVCrud(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestKVCrud(t, s)
}

func TestDynamoDBReaderIsolation(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestReaderIsolation(t, s)
}

func TestDynamoDBReaderOwnsGetBytes(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestDynamoDBWriterOwnsBytes(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestDynamoDBPrefixIterator(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestPrefixIterator(t, s)
}

func TestDynamoDBPrefixIteratorSeek(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestDynamoDBRangeIterator(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestRangeIterator(t, s)
}

func TestDynamoDBRangeIteratorSeek(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestDynamoDBMerge(t *testing.T) {
	s, tableName := open(t, nil)
	defer cleanup(t, s, tableName)
	test.CommonTestMerge(t, s)
}

//TODO: Add config test.

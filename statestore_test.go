package kinesumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/stretchr/testify/assert"
)

func newTestDynamoDB(t *testing.T) *dynamo.DB {
	awsCfg := aws.NewConfig()
	awsCfg.WithRegion("ap-southeast-1")
	awsCfg.WithEndpoint("http://localhost:4566")
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	return dynamo.New(sess)
}

func cleanUpStateStore(t *testing.T, store *stateStore) {
	type PkSk struct {
		PK string `dynamodbav:"pk"`
		SK string `dynamodbav:"sk"`
	}

	_, err := store.db.client.BatchExecuteStatement(context.Background(), &dynamodb.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{
				Statement: aws.String(fmt.Sprintf("DELETE FROM \"%v\"", store.db.table)),
			},
		},
	})
	if err != nil {
		t.Fatal("failed to delete all test data:", err.Error())
	}
}

func TestStateStore_UpdateCheckPointsWorksFine(t *testing.T) {
	cfg := &Config{
		App:              "test",
		DynamoDBRegion:   "ap-southeast-1",
		DynamoDBTable:    "kinesumer-state-store",
		DynamoDBEndpoint: "http://localhost:4566",
	}
	store, err := newStateStore(context.Background(), cfg)
	assert.NoError(t, err, "there should be no error")

	s, _ := store.(*stateStore)
	defer cleanUpStateStore(t, s)

	expectedUpdatedAt := time.Date(2022, 7, 12, 12, 35, 0, 0, time.UTC)

	expected := []*stateCheckPoint{
		{
			StreamKey:      buildCheckPointKey("test", "foobar"),
			ShardID:        "shardId-000",
			SequenceNumber: "0",
			LastUpdate:     expectedUpdatedAt,
		},
	}

	err = s.UpdateCheckPoints(
		context.Background(),
		[]*ShardCheckPoint{
			{
				Stream:         "foobar",
				ShardID:        "shardId-000",
				SequenceNumber: "0",
				UpdatedAt:      expectedUpdatedAt,
			},
		},
	)
	if assert.NoError(t, err, "there should be no error") {
		assert.Eventually(
			t,
			func() bool {
				var result []*stateCheckPoint
				var op *dynamodb.BatchExecuteStatementOutput
				op, err = s.db.client.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
					Statements: []types.BatchStatementRequest{
						{
							Statement: aws.String(
								fmt.Sprintf("SELECT * FROM \"%v\" WHERE pk=%q AND sk=%q", s.db.table,
									buildCheckPointKey("test", "foobar"), "shardId-000")),
						},
						{
							Statement: aws.String(
								fmt.Sprintf("SELECT * FROM \"%v\" WHERE pk=%q AND sk=%q", s.db.table,
									buildCheckPointKey("test", "foo"), "shardId-001")),
						},
					},
				})

				for _, response := range op.Responses {
					var checkPoint stateCheckPoint
					err = attributevalue.UnmarshalMap(response.Item, &checkPoint)
					if err != nil {
						break
					}
				}

				o, err := s.db.client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
					RequestItems: map[string]types.KeysAndAttributes{
						s.db.table: {
							Keys: []map[string]types.AttributeValue{
								{
									"pk": &types.AttributeValueMemberS{Value: buildCheckPointKey("test", "foobar")},
									"sk": &types.AttributeValueMemberS{Value: "shardId-000"},
								},
								{
									"pk": &types.AttributeValueMemberS{Value: buildCheckPointKey("test", "foo")},
									"sk": &types.AttributeValueMemberS{Value: "shardId-001"},
								},
							},
						},
					},
				})

				assert.NoError(t, err)

				assert.Equal(t, len(expected), len(o.Responses))

				for _, v := range o.Responses {
					err = attributevalue.UnmarshalListOfMaps(v, &result)
					assert.NoError(t, err)
				}
				// err := s.db.table.
				// 	Batch("pk", "sk").
				// 	Get(
				// 		[]dynamo.Keyed{
				// 			dynamo.Keys{buildCheckPointKey("test", "foobar"), "shardId-000"},
				// 			dynamo.Keys{buildCheckPointKey("test", "foo"), "shardId-001"},
				// 		}...,
				// 	).All(&result)
				if assert.NoError(t, err) {
					return assert.EqualValues(t, expected, result)
				}
				return false
			},
			600*time.Millisecond,
			100*time.Millisecond,
			"they should be equal",
		)
	}
}

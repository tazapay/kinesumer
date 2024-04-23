package kinesumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
	"github.com/tazapay/grpc-framework/client/session"
	"github.com/tazapay/grpc-framework/env"
)

//go:generate mockgen -source=statestore.go -destination=statestore_mock.go -package=kinesumer StateStore

// Error codes.
var (
	ErrNoShardCache  = errors.New("kinesumer: shard cache not found")
	ErrEmptyShardIDs = errors.New("kinesumer: empty shard ids given")
)

type (
	// StateStore is a distributed key-value store for managing states.
	StateStore interface {
		GetShards(ctx context.Context, stream string) (Shards, error)
		UpdateShards(ctx context.Context, stream string, shards Shards) error
		ListAllAliveClientIDs(ctx context.Context) ([]string, error)
		RegisterClient(ctx context.Context, clientID string) error
		DeregisterClient(ctx context.Context, clientID string) error
		PingClientAliveness(ctx context.Context, clientID string) error
		PruneClients(ctx context.Context) error
		ListCheckPoints(ctx context.Context, stream string, shardIDs []string) (map[string]string, error)
		UpdateCheckPoints(ctx context.Context, checkpoints []*ShardCheckPoint) error
	}

	db struct {
		client *dynamodb.Client
		table  string
	}

	// stateStore implements the StateStore with AWS DynamoDB. (default)
	stateStore struct {
		app string
		db  *db
	}
)

// newStateStore initializes the state store.
func newStateStore(ctx context.Context, cfg *Config) (StateStore, error) {
	awsCfg := session.NewAWS()

	// Ping-like request to check if client can reach to DynamoDB.
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if env.Get(env.Environment) == env.Local {
			o.Region = env.LocalRegion
			o.BaseEndpoint = aws.String(cfg.DynamoDBEndpoint)
		}
	})

	table := cfg.DynamoDBTable
	if _, err := client.DescribeTable(
		ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)},
	); err != nil {
		return nil, errors.Wrap(err, "kinesumer: client can't access to dynamodb")
	}
	return &stateStore{
		app: cfg.App,
		db: &db{
			client: client,
			table:  table,
		},
	}, nil
}

// TableExists determines whether a DynamoDB table exists.
func (s *stateStore) TableExists(ctx context.Context) (bool, error) {
	exists := true
	_, err := s.db.client.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(s.db.table)},
	)
	if err != nil {
		var notFoundEx *types.ResourceNotFoundException
		if errors.As(err, &notFoundEx) {
			log.Printf("Table %v does not exist.\n", s.db.table)
		} else {
			log.Println("Couldn't determine existence of table . Here's why: ", err, "table_name", s.db.table)
		}
		exists = false
	}
	return exists, errors.WithStack(err)
}

// GetShards fetches a cached shard list.
func (s *stateStore) GetShards(
	ctx context.Context, stream string,
) (Shards, error) {
	var (
		key   = buildShardCacheKey(s.app)
		cache *stateShardCache
	)
	o, err := s.db.client.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
			"sk": &types.AttributeValueMemberS{Value: stream},
		},
		TableName:      aws.String(s.db.table),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		log.Println("failed to get shards", err)
		return nil, errors.WithStack(err)
	}

	err = attributevalue.UnmarshalMap(o.Item, &cache)
	if err != nil {
		log.Println("failed to unmarshal shards", err)
		return nil, errors.WithStack(err)
	}

	return cache.Shards, nil
}

// UpdateShards updates a shard list cache.
func (s *stateStore) UpdateShards(
	ctx context.Context, stream string, shards Shards,
) error {
	key := buildShardCacheKey(s.app)
	update := expression.Set(expression.Name("shards"), expression.Value(shards))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		log.Println("failed to build expression for UpdateShards", err)
		return errors.WithStack(err)
	}

	_, err = s.db.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.db.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
			"sk": &types.AttributeValueMemberS{Value: stream},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueUpdatedNew,
	})
	if err != nil {
		log.Println("failed to update shards", err)
		return errors.WithStack(err)
	}

	return nil
}

// ListAllAliveClientIDs fetches an id list of all alive clients.
func (s *stateStore) ListAllAliveClientIDs(ctx context.Context) ([]string, error) {
	var (
		key      = buildClientKey(s.app)
		now      = time.Now().UTC()
		clients  []*stateClient
		response *dynamodb.QueryOutput
	)

	keyEx := expression.Key("pk").Equal(expression.Value(key))
	// keyEx.And(expression.KeyGreaterThan(expression.Key("sk"), expression.Value(" ")))
	keyEx.And(expression.KeyGreaterThan(expression.Key("last_update"), expression.Value(now.Add(-outdatedGap))))
	expr, err := expression.NewBuilder().
		WithKeyCondition(keyEx).
		Build()
	if err != nil {
		log.Printf("Couldn't build expression for ListAllAliveClientIDs query. Here's why: %v\n", err)
		return nil, errors.WithStack(err)
	}

	queryPaginator := dynamodb.NewQueryPaginator(s.db.client, &dynamodb.QueryInput{
		TableName:                 aws.String(s.db.table),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ScanIndexForward:          aws.Bool(true),
	})
	for queryPaginator.HasMorePages() {
		response, err = queryPaginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("Couldn't query for movies released in %v. Here's why: %v\n", key, err)
			return nil, errors.WithStack(err)
		}
		var clientPage []*stateClient
		err = attributevalue.UnmarshalListOfMaps(response.Items, &clientPage)
		if err != nil {
			log.Printf("Couldn't unmarshal query response. Here's why: %v\n", err)
			return nil, errors.WithStack(err)
		}
		clients = append(clients, clientPage...)
	}

	// err := s.db.table.
	// 	Get("pk", key).
	// 	Range("sk", dynamo.Greater, " ").
	// 	Filter("last_update > ?", now.Add(-outdatedGap)).
	// 	Order(dynamo.Ascending).
	// 	AllWithContext(ctx, &clients)

	var ids []string
	for _, client := range clients {
		ids = append(ids, client.ClientID)
	}
	return ids, nil
}

// RegisterClient registers a client to state store.
func (s *stateStore) RegisterClient(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now().UTC()
	)
	client := stateClient{
		ClientKey:  key,
		ClientID:   clientID,
		LastUpdate: now,
	}
	item, err := attributevalue.MarshalMap(client)
	if err != nil {
		log.Println("failed to marshal client data", err)
		return errors.WithStack(err)
	}

	if _, err := s.db.client.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(s.db.table),
	}); err != nil {
		log.Println("failed to register client", err)
		return errors.WithStack(err)
	}
	// if err := s.db.table.Put(client).RunWithContext(ctx)
	return nil
}

// DeregisterClient de-registers a client from the state store.
func (s *stateStore) DeregisterClient(
	ctx context.Context, clientID string,
) error {
	key := buildClientKey(s.app)
	_, err := s.db.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.db.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
			"sk": &types.AttributeValueMemberS{Value: clientID},
		},
	})
	if err != nil {
		log.Printf("Couldn't delete %v from the table. Here's why: %v\n", key, err)
		return errors.WithStack(err)
	}
	return nil
	// err := s.db.table.
	// 	Delete("pk", key).
	// 	Range("sk", clientID).
	// 	RunWithContext(ctx)
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
}

func (s *stateStore) PingClientAliveness(
	ctx context.Context, clientID string,
) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now().UTC()
		err error
	)
	update := expression.Set(expression.Name("last_update"), expression.Value(now))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		log.Println("failed to build ping expression", err)
		return errors.WithStack(err)
	}

	_, err = s.db.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.db.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: key},
			"sk": &types.AttributeValueMemberS{Value: clientID},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})
	// err := s.db.table.
	// 	Update("pk", key).
	// 	Range("sk", clientID).
	// 	Set("last_update", now).
	// 	RunWithContext(ctx)
	if err != nil {
		log.Println("failed to update client aliveness", err)
		return errors.WithStack(err)
	}
	return nil
}

// PruneClients prune clients that have been inactive for a certain amount of time.
func (s *stateStore) PruneClients(ctx context.Context) error {
	var (
		key = buildClientKey(s.app)
		now = time.Now().UTC()
	)
	var outdated []*stateClient

	keyEx := expression.Key("pk").Equal(expression.Value(key))
	keyEx.And(expression.KeyLessThan(expression.Key("last_update"), expression.Value(now.Add(-outdatedGap))))

	expr, err := expression.
		NewBuilder().
		WithKeyCondition(keyEx).
		// WithCondition(conditionEx).
		Build()
	if err != nil {
		log.Println("failed to build prune clients expression", err)
		return errors.WithStack(err)
	}

	o, err := s.db.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(s.db.table),
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    expr.KeyCondition(),
		IndexName:                 aws.String("index-client-key-last-update"),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		log.Println("failed to query outdated client's data", err, expr)
		return errors.WithStack(err)
	}

	err = attributevalue.UnmarshalListOfMaps(o.Items, &outdated)
	if err != nil {
		log.Println("failed to unmarshal outdated clients", err)
		return errors.WithStack(err)
	}

	// err := s.db.table.
	// 	Get("pk", key).
	// 	Range("last_update", dynamo.Less, now.Add(-outdatedGap)).
	// 	Index("index-client-key-last-update").
	// 	AllWithContext(ctx, &outdated)

	if len(outdated) == 0 {
		return nil
	}

	statementRequests := make([]types.BatchStatementRequest, len(outdated))
	for index, client := range outdated {
		params, err := attributevalue.MarshalList([]interface{}{client.ClientKey, client.ClientID})
		if err != nil {
			log.Println("failed to marshal client params", err)
			return errors.WithStack(err)
		}

		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(
				fmt.Sprintf("DELETE FROM \"%v\" WHERE pk=? AND sk=?", s.db.table)),
			Parameters: params,
		}
	}

	_, err = s.db.client.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	if err != nil {
		log.Println("failed to delete clients", err)
		return errors.WithStack(err)
	}

	// var keys []dynamo.Keyed
	// for _, client := range outdated {
	// 	keys = append(
	// 		keys, dynamo.Keys{client.ClientKey, client.ClientID},
	// 	)
	// }

	// _, err = s.db.table.
	// 	Batch("pk", "sk").
	// 	Write().
	// 	Delete(keys...).
	// 	RunWithContext(ctx)
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	return nil
}

// ListCheckPoints fetches check point sequence numbers for multiple shards.
func (s *stateStore) ListCheckPoints(
	ctx context.Context, stream string, shardIDs []string,
) (map[string]string, error) {
	if len(shardIDs) == 0 {
		return nil, ErrEmptyShardIDs
	}

	statementRequests := make([]types.BatchStatementRequest, len(shardIDs))

	for index, id := range shardIDs {
		params, err := attributevalue.MarshalList([]interface{}{buildCheckPointKey(s.app, stream), id})
		if err != nil {
			log.Println("failed to marshal checkpoint params", err)
			return nil, errors.WithStack(err)
		}

		statementRequests[index] = types.BatchStatementRequest{
			Statement: aws.String(
				fmt.Sprintf("SELECT * FROM \"%v\" WHERE pk=? AND sk=?", s.db.table)),
			Parameters: params,
		}
	}

	output, err := s.db.client.BatchExecuteStatement(context.TODO(), &dynamodb.BatchExecuteStatementInput{
		Statements: statementRequests,
	})
	if err != nil {
		log.Println("failed to get checkpoints", err)
		return nil, errors.WithStack(err)
	}

	seqMap := make(map[string]string)

	for _, response := range output.Responses {
		var checkPoint stateCheckPoint
		err = attributevalue.UnmarshalMap(response.Item, &checkPoint)
		if err != nil {
			log.Printf("Couldn't unmarshal response. Here's why: %v\n", err)
			return nil, errors.WithStack(err)
		}
		seqMap[checkPoint.ShardID] = checkPoint.SequenceNumber
	}

	// err := s.db.table.
	// 	Batch("pk", "sk").
	// 	Get(keys...).
	// 	AllWithContext(ctx, &checkPoints)
	// if errors.Is(err, dynamo.ErrNotFound) {
	// 	return seqMap, nil
	// } else if err != nil {
	// 	return nil, errors.WithStack(err)
	// }

	return seqMap, nil
}

// UpdateCheckPoints updates the check point sequence numbers for multiple shards.
func (s *stateStore) UpdateCheckPoints(ctx context.Context, checkpoints []*ShardCheckPoint) error {
	var err error
	written := 0
	batchSize := 25 // DynamoDB allows a maximum batch size of 25 items.
	start := 0
	end := start + batchSize
	for start < len(checkpoints) {
		writeReqs := make([]types.WriteRequest, 0)
		if end > len(checkpoints) {
			end = len(checkpoints)
		}
		for _, checkpoint := range checkpoints[start:end] {
			var item map[string]types.AttributeValue
			scp := stateCheckPoint{
				StreamKey:      buildCheckPointKey(s.app, checkpoint.Stream),
				ShardID:        checkpoint.ShardID,
				SequenceNumber: checkpoint.SequenceNumber,
				LastUpdate:     checkpoint.UpdatedAt,
			}

			item, err = attributevalue.MarshalMap(scp)
			if err != nil {
				log.Printf("Couldn't marshal checkpoint %v for batch writing. Here's why: %v\n", checkpoint, err)
				return errors.WithStack(err)
			}

			log.Println("item", item)

			writeReqs = append(
				writeReqs,
				types.WriteRequest{PutRequest: &types.PutRequest{Item: item}},
			)
		}

		_, err = s.db.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{s.db.table: writeReqs},
		})
		if err != nil {
			log.Printf("Couldn't add a batch of checkpoints to %v. Here's why: %v\n", s.db.table, err)
			return errors.WithStack(err)
		}

		written += len(writeReqs)
		start = end
		end += batchSize
	}

	return nil

	// stateCheckPoints := make([]interface{}, len(checkpoints))

	// for i, checkpoint := range checkpoints {
	// 	stateCheckPoints[i] = stateCheckPoint{
	// 		StreamKey:      buildCheckPointKey(s.app, checkpoint.Stream),
	// 		ShardID:        checkpoint.ShardID,
	// 		SequenceNumber: checkpoint.SequenceNumber,
	// 		LastUpdate:     checkpoint.UpdatedAt,
	// 	}
	// }

	// // TODO(proost): check written bytes
	// _, err := s.db.table.
	// 	Batch("pk", "sk").
	// 	Write().
	// 	Put(stateCheckPoints...).
	// 	RunWithContext(ctx)
}

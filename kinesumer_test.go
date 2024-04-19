package kinesumer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	// dTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kTypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
)

type testEnv struct {
	kinesis      *kinesis.Client
	stateStoreDB *dynamodb.Client
	table        string
	client1      *Kinesumer
	client2      *Kinesumer
	client3      *Kinesumer
}

func newTestEnv(t *testing.T) *testEnv {
	awsCfg, _ := config.LoadDefaultConfig(context.TODO())
	awsCfg.Region = "ap-southeast-1"
	awsCfg.BaseEndpoint = aws.String("http://localhost:14566")
	// sess, err := session.NewSession(awsCfg)
	// if err != nil {
	// 	t.Fatal("failed to init test env:", err.Error())
	// }
	var (
		kinesisClient = kinesis.NewFromConfig(awsCfg)
		stateStoreDB  = dynamodb.NewFromConfig(awsCfg)
	)

	config := &Config{
		App:              "test_client",
		KinesisRegion:    "ap-southeast-1",
		KinesisEndpoint:  "http://localhost:14566",
		DynamoDBRegion:   "ap-southeast-1",
		DynamoDBTable:    "kinesumer-state-store",
		DynamoDBEndpoint: "http://localhost:14566",
		ScanLimit:        10,
		ScanTimeout:      3 * time.Second,
	}

	client1, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	client2, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}
	client3, err := NewKinesumer(config)
	if err != nil {
		t.Fatal("failed to init test env:", err.Error())
	}

	// Drain the errors.
	go func() {
		for {
			select {
			case <-client1.Errors():
			case <-client2.Errors():
			case <-client3.Errors():
			}
		}
	}()

	return &testEnv{
		kinesis:      kinesisClient,
		stateStoreDB: stateStoreDB,
		client1:      client1,
		client2:      client2,
		client3:      client3,
		table:        "kinesumer-state-store",
	}
}

func (e *testEnv) cleanUp(t *testing.T) {
	defer e.client1.Close()
	defer e.client2.Close()
	defer e.client3.Close()

	_, err := e.stateStoreDB.BatchExecuteStatement(context.Background(), &dynamodb.BatchExecuteStatementInput{
		Statements: []types.BatchStatementRequest{
			{
				Statement: aws.String(fmt.Sprintf("DELETE FROM \"%v\"", e.table)),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// if err := table.Scan().All(&pksks); err != nil {
	// 	t.Fatal("failed to scan the state table:", err.Error())
	// }
	// for _, pksk := range pksks {
	// 	keys = append(keys, &dynamo.Keys{pksk.PK, pksk.SK})
	// }
	// if _, err := table.
	// 	Batch("pk", "sk").
	// 	Write().
	// 	Delete(keys...).
	// 	Run(); err != nil {
	// 	t.Fatal("failed to delete all test data:", err.Error())
	// }
}

func (e *testEnv) produceEvents(t *testing.T) {
	_, err := e.kinesis.PutRecords(context.Background(),
		&kinesis.PutRecordsInput{
			Records: []kTypes.PutRecordsRequestEntry{
				{
					Data:         []byte("raw data one"),
					PartitionKey: aws.String("pkey one"),
				},
				{
					Data:         []byte("raw data two"),
					PartitionKey: aws.String("pkey two"),
				},
			},
			StreamName: aws.String("events"),
		},
	)
	if err != nil {
		t.Fatal("failed to produce test events:", err.Error())
	}
}

func TestKinesumer_Consume(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	timeout := time.After(90 * time.Second)

	streams := []string{"events"}
	records1, err := env.client1.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	records2, err := env.client2.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	records3, err := env.client3.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	env.produceEvents(t)

	var (
		records = make(chan *Record)
		stop    = make(chan struct{})
	)

	go func() {
		for {
			select {
			case r := <-records1:
				records <- r
			case r := <-records2:
				records <- r
			case r := <-records3:
				records <- r
			case <-stop:
				close(records)
				return
			}
		}
	}()

	var recv int
	for {
		select {
		case <-records:
			if recv++; recv == 2 {
				stop <- struct{}{}
				return
			}
		case <-timeout:
			t.Errorf("%s timed out", t.Name())
			return
		}
	}
}

func TestShardsRebalancing(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	var err error
	streams := []string{"events"}
	_, err = env.client1.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client2.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client3.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	var (
		clientIDs = []string{
			env.client1.id,
			env.client2.id,
			env.client3.id,
		}

		clients = map[string]*Kinesumer{
			env.client1.id: env.client1,
			env.client2.id: env.client2,
			env.client3.id: env.client3,
		}
	)
	sort.Strings(clientIDs)

	time.Sleep(2*syncInterval + 10*time.Millisecond)

	// expectedShardRanges1 := [][]string{
	// 	{
	// 		"shardId-000000000012", "shardId-000000000013", "shardId-000000000014",
	// 	},
	// 	{
	// 		"shardId-000000000015", "shardId-000000000016",
	// 	},
	// 	{
	// 		"shardId-000000000017", "shardId-000000000018", "shardId-000000000019",
	// 	},
	// }

	// for i, id := range clientIDs {
	// 	shardIDs := clients[id].shards["events"].ids()
	// 	log.Println("@@@@", "id", id, shardIDs)
	// 	expected := expectedShardRanges1[i]
	// 	if !collection.EqualsSS(shardIDs, expected) {
	// 		t.Errorf(
	// 			"expected %v, got %v", expected, shardIDs,
	// 		)
	// 	}
	// }

	// Update kinesis shard count.
	_, err = env.kinesis.UpdateShardCount(context.Background(),
		&kinesis.UpdateShardCountInput{
			ScalingType:      kTypes.ScalingTypeUniformScaling,
			StreamName:       aws.String("events"),
			TargetShardCount: aws.Int32(6),
		},
	)
	if err != nil {
		t.Fatal("failed to update shard count:", err.Error())
	}

	time.Sleep(2*syncInterval + 10*time.Millisecond)

	expectedCount := 6
	// After auto shard rebalancing.
	for _, id := range clientIDs {
		shardIDs := clients[id].shards["events"].ids()
		expectedCount -= len(shardIDs)
	}
	if expectedCount != 0 {
		t.Errorf(
			"expected %v, got %v", 6, 6+expectedCount,
		)
	}
}

func TestKinesumer_MarkRecordWorksFine(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	streams := []string{"events"}
	_, err := env.client1.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	expectedSeqNum := "12345"
	shardIDs := env.client1.shards["events"].ids()
	for _, shardID := range shardIDs {
		env.client1.MarkRecord(&Record{
			Stream:  "events",
			ShardID: shardID,
			Record: kTypes.Record{
				SequenceNumber: &expectedSeqNum,
			},
		})
	}

	for _, shardID := range shardIDs {
		resultSeqNum, ok := env.client1.offsets["events"].Load(shardID)
		if ok {
			assert.EqualValues(t, expectedSeqNum, resultSeqNum, "they should be equal")
		} else {
			t.Errorf("expected %v, got %v", expectedSeqNum, resultSeqNum)
		}
	}
}

func TestKinesumer_MarkRecordFails(t *testing.T) {
	testCases := []struct {
		kinesumer *Kinesumer
		input     *Record
		wantErr   error
		name      string
	}{
		{
			name: "when input record is nil",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input:   nil,
			wantErr: errMarkNilRecord,
		},
		{
			name: "when record sequence number is empty",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
			},
			input: &Record{
				Stream:  "foobar",
				ShardID: "shardId-000",
				Record: kTypes.Record{
					SequenceNumber: func() *string {
						emptyString := ""
						return &emptyString
					}(),
				},
			},
			wantErr: ErrEmptySequenceNumber,
		},
		{
			name: "when unknown stream is given",
			kinesumer: &Kinesumer{
				errors: make(chan error, 1),
				checkPoints: map[string]*sync.Map{
					"foobar": {},
				},
			},
			input: &Record{
				Stream:  "foo",
				ShardID: "shardId-000",
				Record: kTypes.Record{
					SequenceNumber: func() *string {
						seq := "0"
						return &seq
					}(),
				},
			},
			wantErr: ErrInvalidStream,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.kinesumer
			kinesumer.MarkRecord(tc.input)

			result := <-kinesumer.errors
			assert.ErrorIs(t, result, tc.wantErr, "there should be an expected error")
		})
	}
}

func TestKinesumer_Commit(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanUp(t)

	streams := []string{"events"}
	_, err := env.client1.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client2.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}
	_, err = env.client3.Consume(context.TODO(), streams)
	if err != nil {
		t.Errorf("expected no errors, got %v", err)
	}

	clients := map[string]*Kinesumer{
		env.client1.id: env.client1,
		env.client2.id: env.client2,
		env.client3.id: env.client3,
	}

	expectedSeqNum := "12345"
	for _, client := range clients {
		shardIDs := client.shards["events"].ids()
		for _, shardID := range shardIDs {
			env.client1.MarkRecord(&Record{
				Stream:  "events",
				ShardID: shardID,
				Record: kTypes.Record{
					SequenceNumber: &expectedSeqNum,
				},
			})
		}
	}

	for _, client := range clients {
		client.Commit()
	}

	for _, client := range clients {
		shardIDs := client.shards["events"].ids()
		checkpoints, _ := client.stateStore.ListCheckPoints(context.Background(), "events", shardIDs)
		for _, checkpoint := range checkpoints {
			assert.EqualValues(t, expectedSeqNum, checkpoint, "sequence number should be equal")
		}
	}
}

func TestKinesumer_commitCheckPointPerStreamWorksFine(t *testing.T) {
	ctrl := gomock.NewController(t)

	input := []*ShardCheckPoint{
		{
			Stream:         "foobar",
			ShardID:        "shardId-0",
			SequenceNumber: "0",
		},
	}

	mockStateStore := NewMockStateStore(ctrl)
	mockStateStore.EXPECT().
		UpdateCheckPoints(gomock.Any(), input).
		Times(1).
		Return(nil)

	offsets := map[string]*sync.Map{}
	offsets["foobar"] = &sync.Map{}
	offsets["foobar"].Store("shardId-0", "0")
	offsets["foobar"].Store("shardId-1", "1")
	kinesumer := &Kinesumer{
		offsets:       offsets,
		commitTimeout: 2 * time.Second,
		stateStore:    mockStateStore,
	}

	kinesumer.commitCheckPointsPerStream("foobar", input)

	select {
	case err := <-kinesumer.Errors():
		assert.NoError(t, err, "there should be no error")
	default:
	}
}

func TestKinesumer_commitCheckPointPerStreamFails(t *testing.T) {
	ctrl := gomock.NewController(t)

	testCases := []struct {
		name         string
		newKinesumer func() *Kinesumer
		input        struct {
			stream      string
			checkpoints []*ShardCheckPoint
		}
		wantErrMsg string
	}{
		{
			name: "when state store fails to update checkpoints",
			newKinesumer: func() *Kinesumer {
				mockStateStore := NewMockStateStore(ctrl)
				mockStateStore.EXPECT().
					UpdateCheckPoints(gomock.Any(), gomock.Any()).
					Times(1).
					Return(errors.New("mock error"))
				return &Kinesumer{
					errors:     make(chan error, 1),
					stateStore: mockStateStore,
				}
			},
			input: struct {
				stream      string
				checkpoints []*ShardCheckPoint
			}{
				stream: "foobar",
				checkpoints: []*ShardCheckPoint{
					{
						Stream:         "foobar",
						ShardID:        "shardId-000",
						SequenceNumber: "0",
					},
				},
			},
			wantErrMsg: "failed to commit on stream: foobar: mock error",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.newKinesumer()
			kinesumer.commitCheckPointsPerStream(tc.input.stream, tc.input.checkpoints)
			result := <-kinesumer.errors
			assert.EqualError(t, result, tc.wantErrMsg, "there should be an expected error")
		})
	}
}

func TestKinesumer_cleanupOffsetsWorksFine(t *testing.T) {
	testCases := []struct {
		name         string
		newKinesumer func() *Kinesumer
		input        struct {
			stream string
			shard  *Shard
		}
		want map[string]map[string]string // stream - shard - sequence number
	}{
		{
			name: "when clean up existing offset",
			newKinesumer: func() *Kinesumer {
				offsets := map[string]*sync.Map{}

				offsets["foobar"] = &sync.Map{}
				offsets["foobar"].Store("shardId-0", "0")

				offsets["foo"] = &sync.Map{}
				offsets["foo"].Store("shardId-1", "1")
				return &Kinesumer{
					offsets: offsets,
				}
			},
			input: struct {
				stream string
				shard  *Shard
			}{
				stream: "foobar",
				shard: &Shard{
					ID: "shardId-0",
				},
			},
			want: map[string]map[string]string{
				"foo": {
					"shardId-1": "1",
				},
			},
		},
		{
			name: "when clean up non-existent stream",
			newKinesumer: func() *Kinesumer {
				offsets := map[string]*sync.Map{}

				offsets["foobar"] = &sync.Map{}
				offsets["foobar"].Store("shardId-0", "10")

				offsets["foo"] = &sync.Map{}
				offsets["foo"].Store("shardId-1", "20")
				return &Kinesumer{
					offsets: offsets,
				}
			},
			input: struct {
				stream string
				shard  *Shard
			}{
				stream: "bar",
				shard: &Shard{
					ID: "shardId-2",
				},
			},
			want: map[string]map[string]string{
				"foobar": {
					"shardId-0": "10",
				},
				"foo": {
					"shardId-1": "20",
				},
			},
		},
		{
			name: "when clean up non-existent shard",
			newKinesumer: func() *Kinesumer {
				offsets := map[string]*sync.Map{}

				offsets["foobar"] = &sync.Map{}
				offsets["foobar"].Store("shardId-0", "10")

				offsets["foo"] = &sync.Map{}
				offsets["foo"].Store("shardId-1", "20")
				return &Kinesumer{
					offsets: offsets,
				}
			},
			input: struct {
				stream string
				shard  *Shard
			}{
				stream: "foo",
				shard: &Shard{
					ID: "shardId-2",
				},
			},
			want: map[string]map[string]string{
				"foobar": {
					"shardId-0": "10",
				},
				"foo": {
					"shardId-1": "20",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kinesumer := tc.newKinesumer()
			kinesumer.cleanupOffsets(tc.input.stream, tc.input.shard)

			result := make(map[string]map[string]string)
			for stream, offsets := range kinesumer.offsets {
				streamResult := make(map[string]string)
				offsets.Range(func(shardID, sequence interface{}) bool {
					streamResult[shardID.(string)] = sequence.(string)
					return true
				})
				result[stream] = streamResult
			}

			for stream, expectedInStream := range tc.want {
				if assert.NotEmpty(t, result[stream]) {
					streamResult := result[stream]
					for expectedShardID, expectedSeqNum := range expectedInStream {
						if assert.NotEmpty(t, streamResult[expectedShardID]) {
							assert.EqualValues(t, streamResult[expectedShardID], expectedSeqNum, "they should be equal")
						}
					}
				}
			}
		})
	}
}

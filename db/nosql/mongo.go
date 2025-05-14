package nosql

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"GOPROJECT/utils"
)

type MaxDataStruct struct {
	MaxTimestamp time.Time
	MaxValues []float32
}


var (
	Storage =cmap.New[*MaxDataStruct]()
	shardStorage  = cmap.New[cmap.ConcurrentMap[string, *MaxDataStruct]]()
	shardMap = cmap.New[*mongo.Database]()
	once     sync.Once
	initialized bool
)

func MongoDatabase(dsn string, SHARD_DB string) (*mongo.Database, error) {
	ctx, _ := context.WithTimeout(context.Background(), 3 * time.Second)
	
	clientOptions := options.Client().ApplyURI(dsn)
	client, _ := mongo.Connect(ctx, clientOptions)
	
	err := client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("mongoDB ping error: %w", err)
	}

	mongoDB := client.Database(SHARD_DB)

	
	return mongoDB, nil
}

func createDataSavedTimeIndex(ctx context.Context, client *mongo.Database) error {
	// DB 이름과 컬렉션 이름을 설정
	collection := client.Collection("StorageTest")

	// 인덱스 키 정의: DataSavedTime 필드를 내림차순 인덱싱 -1
	indexKey := bson.D{{"DataSavedTime", -1}}

	// IndexModel 생성
	indexModel := mongo.IndexModel{
		Keys: indexKey,
		Options: options.Index().SetName("data_saved_time_1"), // 인덱스 이름 지정 (예시)
		// Background: options.Bool(true), // 백그라운드 인덱스 생성 (큰 컬렉션에 유리)
	}

	name, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		// 이미 동일한 인덱스가 존재하면 에러가 발생하지 않고 name에 기존 인덱스 이름이 반환돼.
		// 다른 에러가 발생했다면 문제가 있는 거지.
		// DuplicateKey 에러 등 특정 에러는 무시할 수도 있지만, 일반적으로 로깅하거나 처리하는 게 좋아.
		// IndexOptionsConflict 에러 등은 기존 인덱스와 옵션이 다를 때 발생해.
		log.Printf("Failed to create index on %s.%s: %v", "StorageTest", "DataSavedTime", err)
        // 이미 존재하는 인덱스 에러는 무시하고 싶다면 에러 타입을 확인해야 해.
        // mongo.Is); err != nil && !mongo.Is(err, mongo.CommandError{Code: 86}) { // 86은 IndexOptionsConflict
        // 	return fmt.Errorf("failed to create index: %w", err)
        // }
        // 단순하게 err != nil 로만 체크해도 됨
        if _, ok := err.(mongo.CommandError); ok {
             // MongoDB CommandError가 발생했다면, 이미 인덱스가 있거나 옵션 충돌일 수 있음
             log.Printf("Index creation might have failed or already exists: %v", err)
             // 필요에 따라 에러 타입을 더 구체적으로 체크하고 처리
        } else {
            // 그 외 다른 에러
            return fmt.Errorf("failed to create index: %w", err)
        }

	} else {
		fmt.Printf("Successfully created index: %s on collection %s\n", name, "StorageTest")
	}
	return nil
}

func InitMongoShard() {
	once.Do(func() {
		SHARD_DB := os.Getenv("SHARD_DB")

		SHARD_ONE_DB_HOST := os.Getenv("SHARD_ONE_DB_HOST")
		SHARD_ONE_DB_PORT := os.Getenv("SHARD_ONE_DB_PORT")
		SHARD_ONE_DB_KEY := os.Getenv("SHARD_ONE_DB_KEY")

		SHARD_TWO_DB_HOST := os.Getenv("SHARD_TWO_DB_HOST")
		SHARD_TWO_DB_PORT := os.Getenv("SHARD_TWO_DB_PORT")
		SHARD_TWO_DB_KEY := os.Getenv("SHARD_TWO_DB_KEY")

		shardDsns := map[string]string{
			SHARD_ONE_DB_KEY: fmt.Sprintf("mongodb://%s:%s", SHARD_ONE_DB_HOST, SHARD_ONE_DB_PORT),
			SHARD_TWO_DB_KEY: fmt.Sprintf("mongodb://%s:%s", SHARD_TWO_DB_HOST, SHARD_TWO_DB_PORT),
		}

		for shardKey, dsn := range shardDsns {
			mongoDB, err := MongoDatabase(dsn, SHARD_DB)
			
			if err != nil {
				utils.HandleError("MongoDB Connection Error : ", err)
				return 
			}

			emptyBuffer := cmap.New[*MaxDataStruct]()
			
			shardMap.Set(shardKey, mongoDB)
			shardStorage.Set(shardKey, emptyBuffer)
		}
		initialized = true
	})
}

func GetShardMap() cmap.ConcurrentMap[string, *mongo.Database] {
    if !initialized {
        InitMongoShard()
    }
    return shardMap
}

// GetShardStorage returns the shard storage instance
func GetShardStorage() cmap.ConcurrentMap[string, cmap.ConcurrentMap[string, *MaxDataStruct]] {
    if !initialized {
        InitMongoShard()
    }
    return shardStorage
}
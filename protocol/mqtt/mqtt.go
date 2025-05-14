package mqtt

import (
	"GOPROJECT/db/nosql"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	cmap "github.com/orcaman/concurrent-map/v2"
)

var (
	JsonMap = cmap.New[interface{}]()
)

func SendMqttMessage() {
	MQTT_PUBLISHER_HOST := os.Getenv("MQTT_PUBLISHER_HOST")
	MQTT_PUBLISHER_PORT := os.Getenv("MQTT_PUBLISHER_PORT")
	MQTT_DSN := fmt.Sprintf("tcp://%s:%s", MQTT_PUBLISHER_HOST, MQTT_PUBLISHER_PORT)
	
	shardMap := nosql.GetShardMap()
	shardStorage := nosql.GetShardStorage()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(MQTT_DSN)
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println(token.Error()) 
	}
	// 각 SHARD KEY에 맞게 MQTT TOPIC 채널에 발행
	for shardKey, _ := range shardMap.Items() {
			ctx, _ := context.WithCancel(context.Background())
			go func(shardKey string){
				fmt.Println(shardKey)
				for {
					select {
					case <-ctx.Done():
						return 
					default:
						time.Sleep(300000 * time.Microsecond)
						shardDevideStorage, _ := shardStorage.Get(shardKey)
						StorageLen := shardDevideStorage.Count()
						if StorageLen > 0 {
							currentTime := time.Now()
							
								// values := make([]interface{}, 0, shardDevideStorage.Count()) // 각 항목당 5개 값
						
								// 형태 ver.T1
								// for key, value := range shardDevideStorage.Items() {
								// 	timeStamp := value.maxTimestamp
								// 	device , _ := Device_Clients.Get(key)
								// 	var snsrData bson.D

								// 	for valIdx , snsrValue := range value.maxValues {
								// 		address := strconv.Itoa(device.dvcInfo.dvc_remap + valIdx)
								// 		snsrData = append(snsrData,
								// 			bson.E{address, snsrValue},
								// 		)
								// 	}

								// 	values = append(values, 
								// 		bson.D{
								// 			{"DeviceId" , key}, 
								// 			{"TimeStamp" , timeStamp}, 
								// 			{"DataSavedTime", currentTime} ,
								// 			{"Value", snsrData},
								// 		},
								// 	)
								// }

								// 형태 ver.2
								for key, value := range shardDevideStorage.Items() {
									JsonMap.Set("DeviceId", key)
									JsonMap.Set("TimeStamp", value.MaxTimestamp)
									JsonMap.Set("DataSavedTime", currentTime)
									JsonMap.Set("Value", value.MaxValues)
									JsonMap.Set("maxTimestamp", value.MaxTimestamp)

									jsonData, err := json.Marshal(JsonMap)
									if err != nil {
										log.Fatalf("JSON 변환 오류: %v", err)
									}
									// JSON
									// jsonString := strin1g(jsonData)
									client.Publish(shardKey, 0, false, jsonData)
									// token.Wait()
									// values = nil
									}

									JsonMap.Clear()
								}
							}
						}
		}(shardKey)
	}
}
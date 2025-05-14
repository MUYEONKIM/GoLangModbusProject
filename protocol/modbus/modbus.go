package modbus

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"GOPROJECT/db/nosql"
	"GOPROJECT/sql"
	"GOPROJECT/utils"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/simonvetter/modbus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// type SqlDeviceDataType struct {
// 	dvc_id       string
// 	dvc_type     string
// 	cmpn_cd      int
// 	dvc_ip       string
// 	dvc_port     string
// 	dvc_remap    int
// 	quantity      int
// 	dvc_interval int
// 	dvc_timeout  int
// 	dvc_slaveid  int
// 	protocol_type string
// 	shard_key    string
// }

type Device struct {
	clientInfo *modbus.ModbusClient
	sucessCount int
	errorCount int
	com int
	dvcInfo sql.SqlDeviceDataType
} 

// 전역 변수 관리
var (
	err error
	SqlDeviceData sql.SqlDeviceDataType

	// map관리
	JsonMap = cmap.New[interface{}]()
	Device_Clients = cmap.New[*Device]()
	DEVICE_CONFIG string
)

// device_client의 com이 0이면 연결, 1이면 무시
// 이거를 go routine으로 실행시키면서 상태확인 및 connect 무한 반복
// start가 되면 이 go 함수가 실행되도록 변경
// read에서 실패하면 com이 0으로 변경이 되어야 함, 여기서 해봤자 무의미

func ModbusInit() {
	SqlDeviceDatas, _ := sql.GetDeviceList()


	if len(SqlDeviceDatas) == 0 {
		utils.HandleError("디바이스 리스트가 존재하지 않습니다.", nil)
	}
	for _, SqlDeviceData := range SqlDeviceDatas {
		Start(SqlDeviceData)
	}
}

func ConnectModbus() error {
	for {
		if !Device_Clients.IsEmpty() {
			for key , Device := range Device_Clients.Items() {
				// 일단 추후 재시작을 고려하기 위해 상태값을 0연결 시도도 안한것, 1 연결 시도했지만 통신 끊어진거, 2 통신하고 있는것으로 구분
				switch Device.com {
				case 0:
					err := Device.clientInfo.Open()

					if err != nil {
						log.Println(key, "error : ", err)
						Device.errorCount++
						continue
					}

					Device.com = 2
					Device.errorCount = 0
					Device.sucessCount++
					
					go ReadModbus(Device)
				case 1:
				case 2:
				}
			}

			time.Sleep(1000 * time.Millisecond)
		}
	}
}

// dvc정보를 하나로 합치고 go routine을 connect와 read를 개별로 두지 말고 일단 하나로 합치자
// 여기서 단순히 map을 만들어주기
func Start(SqlDeviceData sql.SqlDeviceDataType) {
	var client *modbus.ModbusClient
	deviceClient , exists := Device_Clients.Get(SqlDeviceData.Dvc_id)

	switch exists {
	case true:
		// 존재하는 경우
		switch deviceClient.com {
		case 1:
			deviceClient.clientInfo.Open()
			deviceClient.com = 2
			go ReadModbus(deviceClient)
		case 2:
			return 
		}
	case false:
		// 존재하지 않는 경우
		client, err = modbus.NewClient(&modbus.ClientConfiguration{
			// URL: "tcp://" + "192.168.100.108" + ":" + "502",
			URL: "tcp://" + SqlDeviceData.Dvc_ip + ":" + SqlDeviceData.Dvc_port,
			Speed : 1000,
			Timeout: 1 * time.Second,
		})
		client.SetUnitId(uint8(SqlDeviceData.Dvc_slaveid))
		client.SetEncoding(modbus.BIG_ENDIAN, modbus.WordOrder(modbus.LOW_WORD_FIRST))
	
		if err != nil {
			log.Println(err)
		}
	
		// // id 통일은 아래
		// // slave id 다른건 아래
		// err := device.SetUnitId(uint8(index + 1))
		Device_Clients.Set(SqlDeviceData.Dvc_id, &Device{
			clientInfo: client,
			dvcInfo: SqlDeviceData,
		})
	}
}

func DisconnectModbus(dvc_id string) error {
	client, stat := Device_Clients.Get(dvc_id)
	if !stat {
		return fmt.Errorf("%+v 디바이스가 존재하지 않습니다.", dvc_id)
	}
	err = client.clientInfo.Close()
	if err != nil {
		fmt.Println(err)
		return err
	// 1로 변경하여 읽기 및 저장이 안 되도록 함
	}
	client.com = 1
	return nil
}

func RestartModbus(dvc_id string) error {
	client, stat := Device_Clients.Get(dvc_id)
	if !stat {
		return fmt.Errorf("%+v 디바이스가 존재하지 않습니다.", dvc_id)
	}
	client.errorCount, client.sucessCount = 0 ,0 
	switch client.com {
		case 0:
			return fmt.Errorf("%+v 연결 재시도 중입니다.", dvc_id)
		case 1:
			// 1로 변경하여 읽기 및 저장이 안 되도록 함
			client.clientInfo.Open()
			client.com = 2

			go ReadModbus(client)
		case 2:
			client.clientInfo.Close()
			client.com = 0
			// 어차피 재시작은 connect go routine에서 관리하고 있음
	}
	return nil
}

// max값 판별 함수 
func MaxOfSensorData(reg64s []float32, currentTime time.Time, maxData nosql.MaxDataStruct) nosql.MaxDataStruct  {
	if len(reg64s) == 0 {
		log.Println("No data")
	}

	if len(maxData.MaxValues) == 0 {
		maxData.MaxValues = reg64s
		maxData.MaxTimestamp = currentTime
	}
	// maxValues가 담기는 이유는, 슬라이스라서 그렇다. 슬라이스는 pass by value로 되더라도, 가리키는 배열의 주소가 복사되므로, 자동으로 pass by pointer가 되는데
	// maxTimestamp는 time.Time이라서 pass by value로 되어서, 값이 복사되어서 들어가게 된다. 따라서 기존의 값이 변경되는 것이 아님 

	for i, value := range reg64s {
		if maxData.MaxValues[i] < value{
			maxData.MaxValues[i] = value
			maxData.MaxTimestamp = currentTime
		}
	}

	return maxData
}

func ReadModbus(device *Device) {
	// maxValues의 정적 길이 할당 이지만 (sql에서 가져온 데이터의 길이만큼 동적 할당)
	var (
		sendMaxDataChan = make(chan nosql.MaxDataStruct)
		err error
		maxData = nosql.MaxDataStruct{
			MaxValues: make([]float32, 0, device.dvcInfo.Quantity),
		}
	)
	shardStorage := nosql.GetShardStorage()
	
	dvcId := device.dvcInfo.Dvc_id
	shardKey := device.dvcInfo.Shard_key
	dvcType := device.dvcInfo.Dvc_type

	readTicker := time.NewTicker(300000 * time.Microsecond)
	shardBuffer , _ := shardStorage.Get(shardKey)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <- ctx.Done():
				shardBuffer.Remove(dvcId)
				return
			case result := <- sendMaxDataChan:
				maxData = result
			case <- readTicker.C:
				copyData := maxData
				var sumHarmonicData float64
				// // THD DATA
				if dvcType == "SE" {
					basicCurrHarmonicData, currHarmonicData := copyData.MaxValues[61], copyData.MaxValues[62: 81]
	
					for _, v := range currHarmonicData {
						sumHarmonicData += math.Pow(float64(v), 2)
					}
				
					currTHD := math.Sqrt(sumHarmonicData) / float64(basicCurrHarmonicData) * 100
					if math.IsNaN(currTHD) {
						currTHD = 0
					}
					copyData.MaxValues = append(copyData.MaxValues, float32(currTHD))
				}

				if _, exist := shardBuffer.Get(dvcId); !exist {
					shardBuffer.Set(dvcId, &copyData)
				}	else {
					storageData, _ := shardBuffer.Get(dvcId)
					*storageData = copyData
				}
				maxData.MaxValues = make([]float32, device.dvcInfo.Quantity)
				maxData.MaxTimestamp = time.Time{}
			}
		}
	}()

	// 0.05초마다 max값 판별
	for {
		// 상태값 관리
		isOpen, _ := Device_Clients.Get(device.dvcInfo.Dvc_id)
		switch isOpen.com {
		case 1:
			cancel()
			return
		case 2:
			sensorConversionData := make([]float32, device.dvcInfo.Quantity)
			currentTime := time.Now()

			switch dvcType {
			case "DX":
				reg64s, readErrors := device.clientInfo.ReadFloat32s(uint16(device.dvcInfo.Dvc_remap + 1), uint16(device.dvcInfo.Quantity), modbus.HOLDING_REGISTER)
				sensorConversionData = reg64s
				if readErrors != nil {
					err = readErrors
				}
			case "SE":
				reg64s, readErrors := device.clientInfo.ReadRegisters(uint16(device.dvcInfo.Dvc_remap - 1), uint16(device.dvcInfo.Quantity), modbus.HOLDING_REGISTER)
				for i, value := range reg64s {
					sensorConversionData[i] = float32(value)
				}
				if readErrors != nil {
					err = readErrors
				}
			}
			if err != nil {
				close(sendMaxDataChan)
				cancel()
				device.com = 0
				readTicker.Stop()
				log.Println("ReadRegister : ",device.dvcInfo.Dvc_id , err)
				return
			}

			sendMaxDataChan <- MaxOfSensorData(sensorConversionData, currentTime, maxData)
			device.sucessCount++

			// mbpool처럼 16bit 두 개를 받아서 32bit로 합치는것이 아니고 32bit 자체로 변환 후 받기 때문에 mbpool에서 address quantity보다 1/2로 줄이기
			// reg64s, err = device.clientInfo.ReadRegisters(uint16(device.dvcInfo.dvc_remap - 1), uint16(device.dvcInfo.quantity), modbus.HOLDING_REGISTER,)

			time.Sleep(time.Duration(device.dvcInfo.Dvc_interval) * time.Millisecond)
		}
	}
}

func SensorBatchInsert(batchsize int) {
	var shardDBSaveWaitGroup sync.WaitGroup
	shardMap := nosql.GetShardMap()
	shardStorage := nosql.GetShardStorage()
	
	// 여기서 shard key가 구분이 되어야 함
	// 이보다 상위로 가게되면 그만큼의 go routine이 생겨버림
	for key, value := range shardMap.Items() {
			ctx, cancel := context.WithCancel(context.Background())
			db := value
			mongoCollection := db.Collection("StorageTest")
			shardDBSaveWaitGroup.Add(1)
			go func(db *mongo.Database, shardKey string){
				defer shardDBSaveWaitGroup.Done()
				
				for {
					select {
					case <-ctx.Done():
						return 
					default:
						time.Sleep(300000 * time.Microsecond)
						shardDevideStorage, _ := shardStorage.Get(shardKey)
						StorageLen := shardDevideStorage.Count()
						if StorageLen > 0 {
							
							if StorageLen > batchsize {
								
								deviceKeys := make([]string, 0, StorageLen)
								for key := range shardDevideStorage.Items() {
									deviceKeys = append(deviceKeys, key)
								}
								
								// 배치 처리를 위한 반복 횟수 계산
								batchCount := StorageLen / batchsize
								if StorageLen % batchsize > 0 {
									batchCount++
								}
								// 배치 단위로 처리
								for b := 0; b < batchCount; b++ {
									
									// 현재 배치의 시작 및 끝 인덱스 계산
									startIdx := b * batchsize
									endIdx := startIdx + batchsize
									if endIdx > StorageLen {
										endIdx = StorageLen
									}
									
									// 여기부터
									go func(startIdx int, endIdx int) {

										currentTime := time.Now()

										values := make([]interface{}, 0, endIdx - startIdx) // 각 항목당 5개 값
										for i := startIdx; i < endIdx; i++ {
											key := deviceKeys[i]
											value, _ := shardDevideStorage.Get(key)
											device , _ := Device_Clients.Get(key)
											timeStamp := value.MaxTimestamp
											var snsrData bson.D
											for valIdx , snsrValue := range value.MaxValues {
												address := strconv.Itoa(device.dvcInfo.Dvc_remap + valIdx)
												snsrData = append(snsrData,
													bson.E{address, snsrValue},
												)
											}

											values = append(values, 
												bson.D{
													{"DeviceId" , key}, 
													{"TimeStamp" , timeStamp}, 
													{"DataSavedTime", currentTime} ,
													{"Value", snsrData},
												},
											)
										}

										_, err := mongoCollection.InsertMany(ctx, values)
										if err != nil {
											log.Println("shardkey ", shardKey, err)
											cancel()
										}

										values = nil
									}(startIdx, endIdx)
								}
							} else {
								values := make([]interface{}, 0, shardDevideStorage.Count()) // 각 항목당 5개 값
								currentTime := time.Now()
						
								for key, value := range shardDevideStorage.Items() {
									timeStamp := value.MaxTimestamp
									device , _ := Device_Clients.Get(key)
									var snsrData bson.D

									for valIdx , snsrValue := range value.MaxValues {
										address := strconv.Itoa(device.dvcInfo.Dvc_remap + valIdx)
										snsrData = append(snsrData,
											bson.E{address, snsrValue},
										)
									}

									values = append(values, 
										bson.D{
											{"DeviceId" , key}, 
											{"TimeStamp" , timeStamp}, 
											{"DataSavedTime", currentTime} ,
											{"Value", snsrData},
										},
									)
								}

								_, err := mongoCollection.InsertMany(ctx, values)
								if err != nil {
									log.Println("shardkey ", shardKey, err)
									cancel()
								}

								values = nil
								}
							}
				
						}
					}
		}(db, key)
	}
	shardDBSaveWaitGroup.Wait()
}
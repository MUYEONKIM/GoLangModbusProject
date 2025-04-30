package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"

	// "encoding/json"
	"fmt"
	"log"
	"math"

	// "math"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	// "math"
	"os"

	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/simonvetter/modbus"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type maxDataStruct struct {
	maxTimestamp time.Time
	maxValues []float32
}


type SqlDeviceDataType struct {
	dvc_id       string
	dvc_type     string
	cmpn_cd      int
	dvc_ip       string
	dvc_port     string
	dvc_remap    int
	quantity      int
	dvc_interval int
	dvc_timeout  int
	dvc_slaveid  int
	protocol_type string
	shard_key    string
}

type Device struct {
	clientInfo *modbus.ModbusClient
	sucessCount int
	errorCount int
	com int
	dvcInfo SqlDeviceDataType
} 

// 전역 변수 관리
var (
	err error
	SqlDeviceData SqlDeviceDataType

	// map관리
	Storage =cmap.New[*maxDataStruct]()
	shardStorage  = cmap.New[cmap.ConcurrentMap[string, *maxDataStruct]]()

	JsonMap = cmap.New[interface{}]()

	Device_Clients = cmap.New[*Device]()
	mysqlDBMap = cmap.New[*sql.DB]()
	shardMap = cmap.New[*mongo.Database]()
	
	DEVICE_CONFIG string
)


// device_client의 com이 0이면 연결, 1이면 무시
// 이거를 go routine으로 실행시키면서 상태확인 및 connect 무한 반복
// start가 되면 이 go 함수가 실행되도록 변경
// read에서 실패하면 com이 0으로 변경이 되어야 함, 여기서 해봤자 무의미
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
func Start(SqlDeviceData SqlDeviceDataType) {
	var client *modbus.ModbusClient
	deviceClient , exists := Device_Clients.Get(SqlDeviceData.dvc_id)

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
			URL: "tcp://" + SqlDeviceData.dvc_ip + ":" + SqlDeviceData.dvc_port,
			Speed : 1000,
			Timeout: 1 * time.Second,
		})
		client.SetUnitId(uint8(SqlDeviceData.dvc_slaveid))
		client.SetEncoding(modbus.BIG_ENDIAN, modbus.WordOrder(modbus.LOW_WORD_FIRST))
	
		if err != nil {
			log.Println(err)
		}
	
		// // id 통일은 아래
		// // slave id 다른건 아래
		// err := device.SetUnitId(uint8(index + 1))
		Device_Clients.Set(SqlDeviceData.dvc_id, &Device{
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
func MaxOfSensorData(reg64s []float32, currentTime time.Time, maxData maxDataStruct) maxDataStruct  {
	if len(reg64s) == 0 {
		log.Println("No data")
	}

	if len(maxData.maxValues) == 0 {
		maxData.maxValues = reg64s
		maxData.maxTimestamp = currentTime
	}
	// maxValues가 담기는 이유는, 슬라이스라서 그렇다. 슬라이스는 pass by value로 되더라도, 가리키는 배열의 주소가 복사되므로, 자동으로 pass by pointer가 되는데
	// maxTimestamp는 time.Time이라서 pass by value로 되어서, 값이 복사되어서 들어가게 된다. 따라서 기존의 값이 변경되는 것이 아님 

	for i, value := range reg64s {
		if maxData.maxValues[i] < value{
			maxData.maxValues[i] = value
			maxData.maxTimestamp = currentTime
		}
	}

	return maxData
}

func ReadModbus(device *Device) {
	// maxValues의 정적 길이 할당 이지만 (sql에서 가져온 데이터의 길이만큼 동적 할당)
	var (
		sendMaxDataChan = make(chan maxDataStruct)
		err error
		maxData = maxDataStruct{
			maxValues: make([]float32, 0, device.dvcInfo.quantity),
		}
	)
	
	dvcId := device.dvcInfo.dvc_id
	shardKey := device.dvcInfo.shard_key
	dvcType := device.dvcInfo.dvc_type

	readTicker := time.NewTicker(300000 * time.Microsecond)
	shardBuffer , _ := shardStorage.Get(shardKey)

	ctx, cancel := context.WithCancel(context.Background())
	defer fmt.Println("종료됬지롱")
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
					basicCurrHarmonicData, currHarmonicData := copyData.maxValues[61], copyData.maxValues[62: 81]
	
					for _, v := range currHarmonicData {
						sumHarmonicData += math.Pow(float64(v), 2)
					}
				
					currTHD := math.Sqrt(sumHarmonicData) / float64(basicCurrHarmonicData) * 100
					if math.IsNaN(currTHD) {
						currTHD = 0
					}
					copyData.maxValues = append(copyData.maxValues, float32(currTHD))
				}

				if _, exist := shardBuffer.Get(dvcId); !exist {
					shardBuffer.Set(dvcId, &copyData)
				}	else {
					storageData, _ := shardBuffer.Get(dvcId)
					*storageData = copyData
				}
				maxData.maxValues = make([]float32, device.dvcInfo.quantity)
				maxData.maxTimestamp = time.Time{}
			}
		}
	}()

	// 0.05초마다 max값 판별
	for {
		// 상태값 관리
		isOpen, _ := Device_Clients.Get(device.dvcInfo.dvc_id)
		switch isOpen.com {
		case 1:
			cancel()
			return
		case 2:
			sensorConversionData := make([]float32, device.dvcInfo.quantity)
			currentTime := time.Now()

			switch dvcType {
			case "DX":
				reg64s, readErrors := device.clientInfo.ReadFloat32s(uint16(device.dvcInfo.dvc_remap + 1), uint16(device.dvcInfo.quantity), modbus.HOLDING_REGISTER)
				sensorConversionData = reg64s
				if readErrors != nil {
					err = readErrors
				}
			case "SE":
				reg64s, readErrors := device.clientInfo.ReadRegisters(uint16(device.dvcInfo.dvc_remap - 1), uint16(device.dvcInfo.quantity), modbus.HOLDING_REGISTER)
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
				log.Println("ReadRegister : ",device.dvcInfo.dvc_id , err)
				return
			}

			sendMaxDataChan <- MaxOfSensorData(sensorConversionData, currentTime, maxData)
			device.sucessCount++

			// mbpool처럼 16bit 두 개를 받아서 32bit로 합치는것이 아니고 32bit 자체로 변환 후 받기 때문에 mbpool에서 address quantity보다 1/2로 줄이기
			// reg64s, err = device.clientInfo.ReadRegisters(uint16(device.dvcInfo.dvc_remap - 1), uint16(device.dvcInfo.quantity), modbus.HOLDING_REGISTER,)

			time.Sleep(time.Duration(device.dvcInfo.dvc_interval) * time.Millisecond)
		}
	}
}

func SensorBatchInsert(batchsize int) {
	var shardDBSaveWaitGroup sync.WaitGroup
	
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
											timeStamp := value.maxTimestamp
											var snsrData bson.D
											for valIdx , snsrValue := range value.maxValues {
												address := strconv.Itoa(device.dvcInfo.dvc_remap + valIdx)
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
									timeStamp := value.maxTimestamp
									device , _ := Device_Clients.Get(key)
									var snsrData bson.D

									for valIdx , snsrValue := range value.maxValues {
										address := strconv.Itoa(device.dvcInfo.dvc_remap + valIdx)
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


func SendMqttMessage() {
	MQTT_PUBLISHER_HOST := os.Getenv("MQTT_PUBLISHER_HOST")
	MQTT_PUBLISHER_PORT := os.Getenv("MQTT_PUBLISHER_PORT")
	MQTT_DSN := fmt.Sprintf("tcp://%s:%s", MQTT_PUBLISHER_HOST, MQTT_PUBLISHER_PORT)
	
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
									JsonMap.Set("TimeStamp", value.maxTimestamp)
									JsonMap.Set("DataSavedTime", currentTime)
									JsonMap.Set("Value", value.maxValues)
									JsonMap.Set("maxTimestamp", value.maxTimestamp)

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

func GetDeviceListOne(dvcId string) (SqlDeviceDataType, error) {
	var SqlDeviceData SqlDeviceDataType
	var query = "SELECT ds.dvc_id, ds.dvc_type, ds.cmpn_cd, ds.dvc_ip, ds.dvc_port, ds.dvc_remap, ds.quantity, ds.dvc_interval, ds.dvc_timeout, ds.dvc_slaveid, ds.protocol_type, cinfo.shard_key FROM t_dvc_save ds LEFT JOIN companyinfo cinfo ON ds.cmpn_cd = cinfo.cmpn_cd where dvc_id = ?"
	mysqlDB, status := mysqlDBMap.Get("MYSQL_DB")

	if !status {
		return SqlDeviceData, fmt.Errorf("MYSQL DB connect error.")
	}

	row := mysqlDB.QueryRow(query, dvcId)

	err := row.Scan(            
		&SqlDeviceData.dvc_id,
		&SqlDeviceData.dvc_type,
		&SqlDeviceData.cmpn_cd,
		&SqlDeviceData.dvc_ip,
		&SqlDeviceData.dvc_port,
		&SqlDeviceData.dvc_remap,
		&SqlDeviceData.quantity,
		&SqlDeviceData.dvc_interval,
		&SqlDeviceData.dvc_timeout,
		&SqlDeviceData.dvc_slaveid,
		&SqlDeviceData.protocol_type,
		&SqlDeviceData.shard_key,
	)

	if err != nil {
		fmt.Println(err)
		return SqlDeviceData, fmt.Errorf("디바이스 정보를 찾을 수 없습니다.")
	}

	fmt.Println(SqlDeviceData)
	return SqlDeviceData, nil

}

func GetDeviceList() ([]SqlDeviceDataType, error) {
	var SqlDeviceDatas []SqlDeviceDataType
	var query = "SELECT ds.dvc_id, ds.dvc_type, ds.cmpn_cd, ds.dvc_ip, ds.dvc_port, ds.dvc_remap, ds.quantity, ds.dvc_interval, ds.dvc_timeout, ds.dvc_slaveid, ds.protocol_type, cinfo.shard_key FROM t_dvc_save ds LEFT JOIN companyinfo cinfo ON ds.cmpn_cd = cinfo.cmpn_cd "
	mysqlDB, status := mysqlDBMap.Get("MYSQL_DB")

	if !status {
		return nil, fmt.Errorf("MYSQL DB connect error.")
	}
	
	rows, err := mysqlDB.Query(query)

	if err != nil {
		log.Println(err)
	}

	for rows.Next() {
		err := rows.Scan(            
			&SqlDeviceData.dvc_id,
			&SqlDeviceData.dvc_type,
			&SqlDeviceData.cmpn_cd,
			&SqlDeviceData.dvc_ip,
			&SqlDeviceData.dvc_port,
			&SqlDeviceData.dvc_remap,
			&SqlDeviceData.quantity,
			&SqlDeviceData.dvc_interval,
			&SqlDeviceData.dvc_timeout,
			&SqlDeviceData.dvc_slaveid,
			&SqlDeviceData.protocol_type,
			&SqlDeviceData.shard_key,)

		if err != nil {
			fmt.Println(err)
		}
		SqlDeviceDatas= append(SqlDeviceDatas, SqlDeviceData)
	}

	return SqlDeviceDatas, nil
}

func handleError(message string, err error) {
	if err!= nil {
		fmt.Printf("오류: %s\n%v\n", message, err)
		fmt.Println("엔터키를 누르면 프로그램이 종료됩니다...")
	} else {
		fmt.Printf("오류: %s\n", message)
		fmt.Println("엔터키를 누르면 프로그램이 종료됩니다...")
	}
    
    // 사용자 입력 기다리기
    reader := bufio.NewReader(os.Stdin)
    _, _ = reader.ReadString('\n') // 엔터 키가 눌릴 때까지 대기
    
    // 프로그램 종료
    os.Exit(1)
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("ENV 파일이 잘못되었습니다.")
	}
	DEVICE_CONFIG = os.Getenv("DEVICE_CONFIG")

	MYSQL_DB_USER := os.Getenv("DB_USER")
	MYSQL_DB_PASSWORD := os.Getenv("DB_PASSWORD")
	MYSQL_DB_HOST := os.Getenv("DB_HOST")
	MYSQL_DB_PORT := os.Getenv("DB_PORT")
	MYSQL_DB_DATABASE := os.Getenv("DB_DATABASE")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT, MYSQL_DB_DATABASE)
	
	mysqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		handleError("MYSQL Connection Error : " , err)
	}

	if err = mysqlDB.Ping(); err != nil {
		handleError("MYSQL Connection Error : " , err)
	}

	if _, exist := mysqlDBMap.Get("MYSQL_DB"); !exist {
		mysqlDBMap.Set("MYSQL_DB", mysqlDB)
	}

	SHARD_DB := os.Getenv("SHARD_DB")

	SHARD_ONE_DB_HOST := os.Getenv("SHARD_ONE_DB_HOST")
	SHARD_ONE_DB_PORT := os.Getenv("SHARD_ONE_DB_PORT")
	SHARD_ONE_DB_KEY := os.Getenv("SHARD_ONE_DB_KEY")

	SHARD_TWO_DB_HOST := os.Getenv("SHARD_TWO_DB_HOST")
	SHARD_TWO_DB_PORT := os.Getenv("SHARD_TWO_DB_PORT")
	SHARD_TWO_DB_KEY := os.Getenv("SHARD_TWO_DB_KEY")

	BATCH_SIZE := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(BATCH_SIZE)

	if err != nil {
		handleError("BATCH_SIZE Error : ", err)
	}

	shardDsns := map[string]string{
		SHARD_ONE_DB_KEY: fmt.Sprintf("mongodb://%s:%s", SHARD_ONE_DB_HOST, SHARD_ONE_DB_PORT),
		SHARD_TWO_DB_KEY: fmt.Sprintf("mongodb://%s:%s", SHARD_TWO_DB_HOST, SHARD_TWO_DB_PORT),
	}

	for shardKey, dsn := range shardDsns {
		ctx, _ := context.WithTimeout(context.Background(), 3 * time.Second)
	
		clientOptions := options.Client().ApplyURI(dsn)
	
		client, _ := mongo.Connect(ctx, clientOptions)

		err = client.Ping(ctx, readpref.Primary())
		if err != nil {
			handleError("MONGODB Error : ", err)
		}
		
		emptyBuffer := cmap.New[*maxDataStruct]()
	
		mongoDB := client.Database(SHARD_DB)
		if _, exist := shardMap.Get(shardKey); !exist {
			shardMap.Set(shardKey, mongoDB)
			shardStorage.Set(shardKey, emptyBuffer)
		}
	}

	// SqlDeviceDatas, err := GetDeviceList()

	if err != nil {
		handleError("DB에서 데이터를 불러오는 데 오류가 발생하였습니다.", err)
	}

	// if len(SqlDeviceDatas) == 0 {
	// 	handleError("디바이스 리스트가 존재하지 않습니다.", nil)
	// }
	// for _, SqlDeviceData := range SqlDeviceDatas {
	// 	Start(SqlDeviceData)
	// }

	go ConnectModbus()

	switch DEVICE_CONFIG {
	case "0":
		fmt.Println("MQTT 데이터 전송 Config로 실행되었습니다")
		go SendMqttMessage()
	case "1":
		fmt.Println("데이터 Sharding 저장 Config로 실행되었습니다")
		go SensorBatchInsert(batchSize)
	case "2":
		fmt.Println("Broker Config로 실행되었습니다")
	}

	// db에서 값을 불러온 후,start 및 env에서 불러온 batchsize를 sensorbatchinsert에 넣어서 실행을 해주는데
	// config에서 현재 프로그램 상태를 여기서도 분기를 해줌

	// main에서는 was만 열어주는 역할로 가는 것이 맞을듯
	// 일단 기본 프로그램으로서의 역할이 되어야 하니깐
	// config가 1,2일시는 웹앱서버는 필요가 없어짐
}

func main() {
	app := fiber.New()

	app.Post("/connect", func(c fiber.Ctx) error {
		type reqBody struct {
			Dvc_Id []string
		}

		var result reqBody

		err := c.Bind().Body(&result)
		
		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("dvc_id를 확인해주세요.")
		}

		if result.Dvc_Id == nil {
			return c.Status(fiber.StatusBadRequest).SendString("dvc_id를 확인해주세요.")
		}


		for _, v := range result.Dvc_Id{
			sqlResult, err := GetDeviceListOne(v)
			// fmt.Println(err)
			// fmt.Printf("%+v \n", sqlResult)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}

			Start(sqlResult)
		}

		return c.SendString("성공적으로 연결되었습니다.")
	})

	app.Post("/disconnect", func(c fiber.Ctx) error {
		type reqBody struct{
			Dvc_Id []string
		}
		var result reqBody
		err := c.Bind().Body(&result)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		for _, v := range result.Dvc_Id{
			err = DisconnectModbus(v)

			if err != nil {
				log.Println(err)
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
		}

		return c.Status(200).SendString("종료되었습니다.")
	})

	app.Post("/restart", func(c fiber.Ctx) error {
		type reqBody struct{
			Dvc_Id []string
		}
		var result reqBody
		err := c.Bind().Body(&result)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		for _, dvc_id := range result.Dvc_Id {
			err = RestartModbus(dvc_id)

			if err != nil {
				log.Println(err)
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
		}
		return c.Status(200).SendString("재시작 되었습니다.")
	})

	app.Get("/log", func(c fiber.Ctx) error {
		fmt.Println(Device_Clients.Count())
		// fmt.Printf("%+v \n", Storage)
		// for key,  val := range Storage{
		// 	fmt.Println(key)
		// 	fmt.Println(*&val.maxTimestamp)
		// 	fmt.Println(*&val.maxValues)
		// }

		// for key, val := range Device_Clients.Items() {
		// 	fmt.Printf("%v : %+v \n",key, val)
		// }
		// val ,_ := shardMap.Get("shard1")

		// fmt.Println(val.Stats())
		res := fmt.Sprintf("디바이스 연결 개수 : %v", Device_Clients.Count())
		return c.Status(200).SendString(res)
	})

	app.Post("/dbinsert", func(c fiber.Ctx) error {
		type insertData struct {
			Dvc_type string
			Dvc_Ip string
			Dvc_Port int
			Cmpncd int
			Dvc_len int
			Dvc_interval int
			Dvc_timeout int
			Dvc_slaveid int
			Protocol_type string
		}

		var result insertData
		var addQueryParameter []string
		var values []interface{}

		err := c.Bind().Body(&result)

		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("잘못된 요청입니다.")
		}

		db, status := mysqlDBMap.Get("MYSQL_DB")

		if !status {
			return c.Status(fiber.StatusBadRequest).SendString("MAIN DB 연결에 실패하였습니다.")
		}

		basequery := "INSERT INTO t_dvc_save (dvc_id, dvc_type, cmpn_cd, dvc_ip, dvc_port, dvc_remap, quantity, dvc_interval, dvc_timeout, dvc_slaveid, protocol_type) VALUES "
		var (
			address, quantity int
		)

		if result.Dvc_type == "DX" {
			address = 1
			quantity = 50
		} else {
			address = 1300
			quantity = 125
		}

		for i := 0; i < result.Dvc_len; i++ {
			dvc_id := fmt.Sprintf("Device%v", result.Dvc_Port + i)
			dvc_type := result.Dvc_type
			cmpn_cd := result.Cmpncd
			dvc_ip := result.Dvc_Ip
			dvc_port := result.Dvc_Port + i
			dvc_remap := address
			quantity := quantity
			dvc_interval := result.Dvc_interval
			dvc_timeout := result.Dvc_timeout
			dvc_slaveid := result.Dvc_slaveid
			protocol_type := result.Protocol_type

			addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			
			values = append(values,
				dvc_id, dvc_type, cmpn_cd, dvc_ip, dvc_port, dvc_remap, quantity, dvc_interval, dvc_timeout, dvc_slaveid, protocol_type,
			)
		}
			
		if len(values) > 0 {
			fmt.Println(len(addQueryParameter))

			sql := basequery + strings.Join(addQueryParameter, ", ")
			stmt, err := db.Prepare(sql)
			// fmt.Println("배치", b+1, "/", batchCount, "- query갯수:", len(addQueryParameter), "values갯수:", len(values))
			if err != nil {
				log.Fatal(err)
			}
			
			_, err = stmt.Exec(values...)
			if err != nil {
				fmt.Println(err)
				log.Fatal(err)
			}
			stmt.Close() 
		}
			// fmt.Println(val, status)

		return nil
	})
	log.Fatal(app.Listen(":3000"))
}
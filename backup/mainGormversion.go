package main

import (
	// "database/sql"
	"fmt"
	"log"

	// "os"
	"strconv"

	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/simonvetter/modbus"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/sharding"
)



type SensorData struct {
	dvcid string
	maxValues []uint16
	maxTimestamp time.Time
}

type maxDataStruct struct {
	maxTimestamp time.Time
	maxValues []uint16
}

type Device struct {
	DeviceInfo *modbus.ModbusClient
	SucessCount int
	ErrorCount int
	Com int
	DvcId string
	addr uint16
	quantity uint16
	count int
	ticker *time.Ticker
} 

var (
	err error
	timestamp time.Time
	mu sync.Mutex
	wg sync.WaitGroup

	// map관리
	// Storage = make(map[string]*maxDataStruct)
	Storage =cmap.New[*maxDataStruct]()
	// Device_Clients = make(map[string]*Device)
	Device_Clients = cmap.New[*Device]()
	// 채널관리
	stopReadChan = make(chan bool)
	saveSnsrChan = make(chan SensorData, 1)
)


// device_client의 com이 0이면 연결, 1이면 무시
// 이거를 go routine으로 실행시키면서 상태확인 및 connect 무한 반복
// start가 되면 이 go 함수가 실행되도록 변경
// read에서 실패하면 com이 0으로 변경이 되어야 함, 여기서 해봤자 무의미
// func ConnectModbus(DvcId string, client *modbus.ModbusClient) error {
func ConnectModbus() error {
	for {
		if !Device_Clients.IsEmpty() {
			for key , Device := range Device_Clients.Items() {
				// fmt.Printf("%v : %+v \n", key, Device)

				// 일단 추후 재시작을 고려하기 위해 상태값을 0연결 시도도 안한것, 1 연결 시도했지만 통신 끊어진거, 2 통신하고 있는것으로 구분
				switch Device.Com {
				case 0:
					err := Device.DeviceInfo.Open()

					if err != nil {
						fmt.Println(key, "error : ", err)
						Device.ErrorCount++
						continue
					}

					Device.Com = 2
					Device.ErrorCount = 0
					Device.SucessCount++
					
					wg.Add(1)

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
func Start(Count int, Batchsize int, ip string, length int, port int, db *gorm.DB) error {
	if length != 0 {
		for portNum := port; portNum < port + length; portNum++ {
			wg.Add(1)
			go func(portNum int){
				var client *modbus.ModbusClient
				ticker := time.NewTicker(300000 * time.Microsecond)

				defer wg.Done()
				DvcId := fmt.Sprintf("Device%v", portNum)
	
				client, err = modbus.NewClient(&modbus.ClientConfiguration{
					// URL: "tcp://" + "192.168.100.108" + ":" + "502",
					URL: "tcp://" + ip + ":" + strconv.Itoa(portNum),
					Speed : 1000,
					Timeout: 1 * time.Second,
				})
				client.SetUnitId(uint8(1))

				if err != nil {
					fmt.Println("에러 : " ,err)
				}

			    // // id 통일은 아래
				// // slave id 다른건 아래
				// err := device.SetUnitId(uint8(index + 1))
				if _, exists := Device_Clients.Get(DvcId); !exists {
					Device_Clients.Set(DvcId, &Device{
						DeviceInfo: client,
						DvcId: DvcId,
						addr : 1300,
						quantity : 125,
						count: Count,
						ticker: ticker,
					})
				}

			}(portNum)
		}

		go ConnectModbus()
		go SensorBatchInsert(Batchsize, db)
	}
	return fmt.Errorf("최소.")
}


// func DisconnectModbus(ip string) error {
// 	for _, client := range Device_Clients.Items() {
// 		err = client.Close()
// 		if err != nil {
// 			fmt.Println(err)
// 			return err
// 		}
// 	}
// 	Device_Client = []*modbus.ModbusClient{}
// 	return nil
// }

// max값 판별 함수 
func MaxOfSensorData(reg64s []uint16, currentTime time.Time, maxData maxDataStruct) maxDataStruct {
	if len(reg64s) == 0 {
		log.Fatal("No data")
		fmt.Println("No data")
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
	var maxData = maxDataStruct{}
	var sendMaxDataChan = make(chan maxDataStruct, 1)

	defer device.ticker.Stop()
	defer wg.Done()
	// MaxCount := 0
	// cnt := 0
	wg.Add(1)

	// 0.3초마다 storage에 데이터 쌓이는 루틴
	go func() {
		defer wg.Done()
		for {
			select {
			case result := <- sendMaxDataChan:
				maxData = result
			case <- device.ticker.C:
				copyData := maxData
				// fmt.Printf("%+v \n", device)

				Storage.Set(device.DvcId, &copyData)
				
				if _, exist := Storage.Get(device.DvcId); !exist {
					Storage.Set(device.DvcId, &copyData)
				}	else {
					storageData, _ := Storage.Get(device.DvcId)
					*storageData = copyData
				}

				// if _, exist := Storage[device.DvcId]; !exist {
				// 	// 주소를 저장 (새롭게 생성)
				// 	Storage[device.DvcId] = &copyData
				// 	} else {
				// 		// 저장된 주소값에 역참조로 데이터를 덮어씌움 (업데이트)
				// 	*(Storage[device.DvcId]) = copyData
				// }
				// cnt++

				maxData.maxValues = make([]uint16, 0)
				maxData.maxTimestamp = time.Time{}
			}
		}
	}()
	// 0.05초마다 max값 판별
	for {
		select {
		case <- stopReadChan:
			fmt.Println("Stop read")
			return
		default:
			// if MaxCount == device.count {
			// 	mu.Lock()
			// 	fmt.Printf("%+v \n", testCount)
			// 	stopReadChan <- true
			// 	wg.Done()
			// 	mu.Unlock()
			// 	return
			// }
			var reg64s []uint16

			reg64s, err = device.DeviceInfo.ReadRegisters(device.addr - 1, device.quantity, modbus.HOLDING_REGISTER)
			
			if err != nil {
				device.Com = 0
				fmt.Printf("%+v",device.DeviceInfo)
				fmt.Println(err, device.DvcId ,"read에서 난 에러")
				return
			}


			device.SucessCount++
			
			currentTime := time.Now()
			var maxDatas = MaxOfSensorData(reg64s, currentTime, maxData)
			sendMaxDataChan <- maxDatas
			// mu.Lock()
			// testCount[device.DvcId]++
			// mu.Unlock()

			// MaxCount++

			// 0.05초 대기
			time.Sleep(50000 * time.Microsecond)
		}
	}
}

func SensorBatchInsert(batchsize int, db *gorm.DB) {
	// 얘가 디바이스 갯수가 될 듯
	basequery := "INSERT INTO t_storage_test (dvc_id, address, value, timestamp, datasavedtime) VALUES "
	for {
		select {
	case <- stopReadChan:
		fmt.Println("Stop Save")
		return 
	default:
		// sleep를 처음에 주는 이유는, 마지막에 존재할 시, 처음에 값을 storage에 저장 시키기도 전에 실행을 먼저 시켜서 첫번째 maxdata가 유실이 되버림
		// 그래서 처음에 0.3초를 먼저 기다린 후에 저장 로직을 하도록 해야 첫번재 maxdata가 저장이 됨
		time.Sleep(300000 * time.Microsecond)
		StorageLen := Storage.Count()
			if StorageLen > 0 {
				currentTime := time.Now()
				var batchwg sync.WaitGroup
				
				if StorageLen > batchsize {

					deviceKeys := make([]string, 0, StorageLen)
					for key := range Storage.Items() {
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
						batchwg.Add(1)
						go func(startIdx int, endIdx int) {
							var values []interface{}
							var addQueryParameter []string
							
							defer batchwg.Done()

							for i := startIdx; i < endIdx; i++ {
								key := deviceKeys[i]
								value, _ := Storage.Get(key)
								// value := Storage[key]
								timeStamp := value.maxTimestamp
								for i, snsrValue := range value.maxValues {
									addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?, ?)")
									address := 1300 + i
									values = append(values, 
										key, 
										address,
										snsrValue,
										timeStamp,
										currentTime,
									)
								}
							}
							
							// 현재 배치 데이터 저장
							if len(values) > 0 {
								sql := basequery + strings.Join(addQueryParameter, ", ")
								// stmt, err := db.Prepare(sql)
								// // fmt.Println("배치", b+1, "/", batchCount, "- query갯수:", len(addQueryParameter), "values갯수:", len(values))
								// if err != nil {
								// 	log.Fatal(err)
								// }

								err := db.Exec(sql, values...)
								
								if err != nil {
									fmt.Println(err, "저장부분1")
									log.Fatal(err, "저장부분1")
								}

								// _, err = stmt.Exec(values...)
								// if err != nil {
								// 	fmt.Println(err)
								// 	log.Fatal(err)
								// }
								// stmt.Close() 
							}
						}(startIdx, endIdx)
						// 현재 배치에 속한 디바이스만 처리
						

						// 여기까지?
					}
				} else {
					var values []interface{}
					var addQueryParameter []string
					
					for key, value := range Storage.Items() {
						timeStamp := value.maxTimestamp
						for i, snsrValue := range value.maxValues {
							addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?, ?)")
							address := 1300 + i
							values = append(values, 
								key, 
								address,
								snsrValue,
								timeStamp,
								currentTime,
							)
						}
					}
					// 전체 데이터 다 저장
					if len(values) > 0 {
						fmt.Println(len(addQueryParameter))
						
						sql := basequery + strings.Join(addQueryParameter, ", ")
						result := db.Exec(sql, values...)
						
						fmt.Println(sql)
						fmt.Println(len(values))
						if result != nil {
							fmt.Printf("%+v", result)
						}
						
						// stmt, err := db.Prepare(sql)
						// if err != nil {
						// 	log.Fatal(err)
						// }
						// defer stmt.Close()
						
						// _, err = stmt.Exec(values...)
						// if err != nil {
						// 	fmt.Println(err)
						// 	log.Fatal(err)
						// }
					}
				}
			}
		}
	}
}


func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

var testCount = make(map[string]int)


func main() {
	// DB_HOST := os.Getenv("DB_HOST")
	// DB_PORT := os.Getenv("DB_PORT")
	// DB_USER := os.Getenv("DB_USER")
	// DB_PASSWORD := os.Getenv("DB_PASSWORD")
	// DB_DATABASE := os.Getenv("DB_DATABASE")

	// dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE)

	// db, err := sql.Open("mysql", dsn)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// db.SetMaxOpenConns(20) // 최대 연결 수 제한
	// db.SetMaxIdleConns(20)  // 유휴 연결 수 제한
	// defer db.Close()

    // if err = db.Ping(); err != nil {
    //     log.Fatal(err)
    // }

	DB_SESSION := []gorm.Dialector{
		mysql.Open("root:its@1234@tcp(192.168.100.88:3306)/admin?charset=utf8mb4&parseTime=True&loc=Local"),
		// mysql.Open("root:its@1234@tcp(192.168.100.83:3306)/admin?charset=utf8mb4&parseTime=True&loc=Local"),
		// mysql.Open("user:password@tcp(192.168.100.85:3306)/database_name?charset=utf8mb4&parseTime=True&loc=Local"),
	}

	db, err := gorm.Open(DB_SESSION[0], &gorm.Config{
		PrepareStmt: true,
	})

	if err != nil {
		fmt.Println("db connect error", " : ",err)
	}

	middleware := sharding.Register(sharding.Config{
		ShardingKey:         "dvc_id", // 샤딩 키 (사용자 ID 기준으로 데이터 분산)
		// NumberOfShards:  len(DB_SESSION), // 샤드 수 (서버 수)
		NumberOfShards: 2,
		PrimaryKeyGenerator: sharding.PKSnowflake, // 기본 키 생성 방식 (Snowflake 알고리즘)
		// ShardingAlgorithm: func(value interface{}) (shardIndex int, err error) {
		// 	// 샤딩 알고리즘: user_id를 샤드 서버 수로 나눈 나머지로 분산
		// 	userID := value.(int64)
		// 	return int(userID % int64(len(dataSources))), nil
		// },
		// Sources: dataSources, // 데이터 소스 목록
	}, "t_storage_test") 

	db.Use(middleware)


	app := fiber.New()

	// 모드버스 통신 값을 읽고 interval 마다 Storage buffer에 데이터를 저장시켜주는 역할
	

	app.Post("/", func(c fiber.Ctx) error {
		type readBody struct {
			Dvc_len int
			Ip string
			Port int
			Count int
			Batchsize int
		}

		var result readBody

		err := c.Bind().Body(&result)
		
		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		err = Start(result.Count, result.Batchsize, result.Ip, result.Dvc_len, result.Port, db)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		return c.SendString("성공적으로 연결되었습니다.")
	})

	app.Post("/readtest", func(c fiber.Ctx) error {
		type readBody struct {
			Count int
			Batchsize int
		}

		var result readBody
		err := c.Bind().Body(&result)

		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		stopReadChan = make(chan bool)

		if 1 != 0 {
			// go SensorBatchInsert(result.Batchsize)
			// for index, device := range Device_Client {
			// 	ticker := time.NewTicker(300 * time.Millisecond)
			// 	wg.Add(1)
			// 	go ReadModbus(index, device, 1300, 125, &wg ,result.Count, result.Batchsize, ticker)
			// }
		} else {
			log.Fatal("연결된 디바이스가 없습니다.")
			return c.SendString("연결된 디바이스가 없습니다")
		}
		return nil
	})

	app.Get("/readstop", func(c fiber.Ctx) error {
		close(saveSnsrChan)
		// close(sendMaxDataChan)
		close(stopReadChan)
		// wg.Wait()
		return c.SendString("읽기를 멈춥니다")
	})

	app.Get("/disconnect", func(c fiber.Ctx) error {
		// err = DisconnectModbus("192.168.100.108")
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		return c.SendString("종료되었습니다.")
	})

	app.Get("/log", func(c fiber.Ctx) error {
		// fmt.Println(len(Device_Clients))
		fmt.Printf("%+v \n", Storage)
		// for key,  val := range Storage{
		// 	fmt.Println(key)
		// 	fmt.Println(*&val.maxTimestamp)
		// 	fmt.Println(*&val.maxValues)
		// }
		return c.SendString("Q")
	})

	log.Fatal(app.Listen(":3000"))
}
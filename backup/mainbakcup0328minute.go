package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"strconv"

	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	"github.com/simonvetter/modbus"
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
}

var (
	err error
	Device_Client []*modbus.ModbusClient
	client *modbus.ModbusClient
	timestamp time.Time
	mu sync.Mutex
	wg sync.WaitGroup

	// map관리
	Storage = make(map[string]*maxDataStruct)
	Device_Clients = make(map[string]*Device)
	
	// 채널관리
	stopReadChan = make(chan bool)
	saveSnsrChan = make(chan SensorData, 1)
	sendMaxDataChan = make(chan maxDataStruct, 1)
)

func ConnectModbus(DvcId string, client *modbus.ModbusClient) error {
	// Open 시 에러가 나는 것이니 에러처리는 아래 쪽
	err = client.Open()


	if err != nil {
		fmt.Println("Device", DvcId, " Error : ",err)
		// 에러카운트 추가 해줘야 할것
		mu.Lock()
		Device_Clients[DvcId].ErrorCount++
		mu.Unlock()
		// time.Sleep(100 * time.Millisecond)

		fmt.Println(Device_Clients[DvcId].ErrorCount)
		if Device_Clients[DvcId].ErrorCount == 3 {
			return fmt.Errorf(DvcId, " 통신 실패로 인한 disconnect")
		}

		return ConnectModbus(DvcId, client)
	}

	mu.Lock()
	Device_Clients[DvcId].Com = 1
	mu.Unlock()
	mu.Lock()
	Device_Clients[DvcId].SucessCount++
	mu.Unlock()
	// 에러 발생시 에러 카운트 하나 증가 시키고, 1초후 reconnect errcnt가 10을 넘기면 disconnect?
	// reconnect 되었을 때 read를 하고 있으면 자동으로 read가 되게 해야할 듯
	return nil
}

// dvc정보를 하나로 합치고 go routine을 connect와 read를 개별로 두지 말고 일단 하나로 합치자

func Start(Count int, Batchsize int, ip string, length int, port int, db *sql.DB) error {
	if length != 0 {
		for portNum := port; portNum < port + length; portNum++ {
			wg.Add(1)
			go func(portNum int){
				defer wg.Done()
				DvcId := fmt.Sprintf("Device%v", portNum)
	
				mu.Lock()
				if _, exists := Device_Clients[DvcId]; !exists {
					Device_Clients[DvcId] = &Device{}
				}
				mu.Unlock()
	
				client, err = modbus.NewClient(&modbus.ClientConfiguration{
					// URL: "tcp://" + ip + ":" + "502",
					URL: "tcp://" + ip + ":" + strconv.Itoa(portNum),
					Speed : 1000,
					Timeout: 1 * time.Second,
				})
			
				mu.Lock()
				Device_Clients[DvcId].DeviceInfo = client
				mu.Unlock()
				
				// // 아래 이것들도 각각의 go 함수로 실행이 되어야 함
				// // 각각의 지역변수 Client가 따로 생기는 것
				// // 현재 그러면 device_clients map에는 mutex로 접근이 되야한다
				// // 에러가 뜨더라도 계속 다시 실행을 해보아야 하기 때문에 일단 key값으로는 만들어진다.
				// // 또한 계속 연결을 반복해야 하기 때문에 몇번이상 err cnt이상이면 통신 끊기 이런건 없음
				// // 그리고 com이라는 객체값을 부여하여, 에러가 발생한 상황이면 com이 0 상태, 통신이 성공하면 1 상태로 해준다
				// // 그럼 ConnectModbus 함수를 아래처럼 분리할 필요가 있을까?
				// // 한군데 합치는 걸로 충분히 가능할 듯 
				err := ConnectModbus(DvcId, client)

				if err != nil {
					for K, v := range Device_Clients {
						fmt.Printf("%v %+v \n",K, v )
					}
					// fmt.Println(err)
				}

				// // id 통일은 아래
				client.SetUnitId(uint8(1))
				// // slave id 다른건 아래
				// // err := device.SetUnitId(uint8(index + 1))


				// ticker := time.NewTicker(300 * time.Millisecond)
				// // 일단은 Com으로 해놓았지만 select case로 연결되었을 때 안되었을 때를 지정해줘야 한다

				// mu.Lock()
				// if Device_Clients[DvcId].Com == 1 {
				// 	ReadModbus(client, DvcId, 1300, 125, &wg ,Count, ticker)
				// }
				// mu.Unlock()

			}(portNum)
			// readtest가 여기에 들어가야 할 것이다
			// Device_Client = append(Device_Client, Client)
			// fmt.Printf("%+v \n",Client)

		// fmt.Println(len(Device_Clinets), "개의 디바이스가 연결되었습니다")
		}
		// go SensorBatchInsert(Batchsize, db)

		// 여기에 save가 들어와야 겠죠?
		// for _, v := range Device_Clients {
		// 	fmt.Printf("%+v \n", v)
		// }
		// fmt.Printf("%+v \n", Device_Clinets)
	}
	return fmt.Errorf("최소.")
}

func DisconnectModbus(ip string) error {
	for _, client := range Device_Client {
		err = client.Close()
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	fmt.Println(len(Device_Client), "개의 디바이스가 종료되었습니다")
	Device_Client = []*modbus.ModbusClient{}
	return nil

}

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

func ReadModbus(client *modbus.ModbusClient, DvcId string,addr, quantity uint16, wg *sync.WaitGroup, count int,  ticker *time.Ticker) {
	var maxData = maxDataStruct{}

	defer ticker.Stop()
	defer wg.Done()
	MaxCount := 0
	cnt := 0
	wg.Add(1)
	// 0.3초마다 storage에 데이터 쌓이는 루틴
	go func() {
		defer wg.Done()

		for {
			select {
			case result := <- sendMaxDataChan:
				maxData = result
			case <- ticker.C:
				copyData := maxData

				mu.Lock()
				
				if _, exist := Storage[DvcId]; !exist {
					// 주소를 저장 (새롭게 생성)
					Storage[DvcId] = &copyData
					} else {
						// 저장된 주소값에 역참조로 데이터를 덮어씌움 (업데이트)
					*(Storage[DvcId]) = copyData
				}
				mu.Unlock()

				cnt++
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
			if MaxCount == count {
				mu.Lock()
				fmt.Printf("%+v \n", testCount)
				stopReadChan <- true
				wg.Done()
				mu.Unlock()
				return
			}
			var reg64s []uint16

			reg64s, err = client.ReadRegisters(addr - 1,quantity,modbus.HOLDING_REGISTER)
			if err != nil {
				fmt.Println(err)
				return
			}

			currentTime := time.Now()
			var maxDatas = MaxOfSensorData(reg64s, currentTime, maxData)
			sendMaxDataChan <- maxDatas
			mu.Lock()
			testCount[DvcId]++
			mu.Unlock()

			MaxCount++

			// 0.05초 대기
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func SensorBatchInsert(batchsize int, db *sql.DB) {
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
		var values []interface{}
		var addQueryParameter []string
		StorageLen := len(Storage)

			if StorageLen > 0 {
				mu.Lock()
				currentTime := time.Now()
				
				if StorageLen > batchsize {

					deviceKeys := make([]string, 0, StorageLen)
					for key := range Storage {
						deviceKeys = append(deviceKeys, key)
					}
					
					// 배치 처리를 위한 반복 횟수 계산
					batchCount := StorageLen / batchsize
					if StorageLen % batchsize > 0 {
						batchCount++
					}
					
					// 배치 단위로 처리
					for b := 0; b < batchCount; b++ {
					var values []interface{}
						var addQueryParameter []string
						
						// 현재 배치의 시작 및 끝 인덱스 계산
						startIdx := b * batchsize
						endIdx := startIdx + batchsize
						if endIdx > StorageLen {
							endIdx = StorageLen
						}
						
						// 현재 배치에 속한 디바이스만 처리
						for i := startIdx; i < endIdx; i++ {
							key := deviceKeys[i]
							value := Storage[key]
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
					}
				} else {
					for key, value := range Storage {

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
						sql := basequery + strings.Join(addQueryParameter, ", ")
						stmt, err := db.Prepare(sql)
						if err != nil {
							log.Fatal(err)
						}
						defer stmt.Close()
						
						_, err = stmt.Exec(values...)
						if err != nil {
							fmt.Println(err)
							log.Fatal(err)
						}
					}
				}
				mu.Unlock()

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
	DB_HOST := os.Getenv("DB_HOST")
	DB_PORT := os.Getenv("DB_PORT")
	DB_USER := os.Getenv("DB_USER")
	DB_PASSWORD := os.Getenv("DB_PASSWORD")
	DB_DATABASE := os.Getenv("DB_DATABASE")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(20) // 최대 연결 수 제한
	db.SetMaxIdleConns(10)  // 유휴 연결 수 제한
	defer db.Close()

    if err = db.Ping(); err != nil {
        log.Fatal(err)
    }

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

		if len(Device_Client) != 0 {
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
		close(sendMaxDataChan)
		close(stopReadChan)
		// wg.Wait()
		return c.SendString("읽기를 멈춥니다")
	})

	app.Get("/disconnect", func(c fiber.Ctx) error {
		err = DisconnectModbus("192.168.100.108")
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		return c.SendString("종료되었습니다.")
	})

	app.Get("/log", func(c fiber.Ctx) error {
		fmt.Println(len(Device_Clients))
		// for key,  val := range Storage{
		// 	fmt.Println(key)
		// 	fmt.Println(*&val.maxTimestamp)
		// 	fmt.Println(*&val.maxValues)
		// }
		return c.SendString("Q")
	})

	log.Fatal(app.Listen(":3000"))
}
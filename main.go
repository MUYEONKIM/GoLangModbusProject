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

type batchType struct {
	snsrValue []uint16
	snsrTimeStamp []time.Time
	dvcId []string
}

type maxDataStruct struct {
	maxTimestamp time.Time
	maxValues []uint16
}

var err error
var (
	Device_Client []*modbus.ModbusClient
	client *modbus.ModbusClient
	timestamp time.Time
	mu sync.Mutex
	wg sync.WaitGroup
)
// map관리
var Storage = make(map[string]*maxDataStruct)

// 채널관리
var stopReadChan = make(chan bool)
var saveSnsrChan = make(chan SensorData, 1)
var sendMaxDataChan = make(chan maxDataStruct, 1)


func ConnectModbus(ip string, port string) *modbus.ModbusClient{
	client, err = modbus.NewClient(&modbus.ClientConfiguration{
		// URL: "tcp://" + ip + ":" + "502",
		URL: "tcp://" + ip + ":" + port,
		Speed : 1000,
		Timeout: 1 * time.Second,
		
	})
	err = client.Open()
	if err != nil {
		fmt.Println(err ,"connectmodbus쪽")
		return nil
	}
	return client
}

func Start(ip string, length int) {
	if length != 0 {
		for port := 30000; port < 30000 + length; port++ {
			Client := ConnectModbus(ip, strconv.Itoa(port))
			Device_Client = append(Device_Client, Client)
			fmt.Printf("%+v \n",Client)
		}
		fmt.Println(len(Device_Client), "개의 디바이스가 연결되었습니다")
		} else {
		fmt.Println("적어도 하나의 디바이스를 연결해야 합니다")
	}
}

func DisconnectModbus(ip string) string {
	for _, client := range Device_Client {
		err = client.Close()
		if err != nil {
			fmt.Println(err)
			return "디바이스 연결을 종료하는데 실패했습니다"
		}
	}
	return "디바이스 연결을 종료합니다"

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

	SensorBatchInsert := func(batchsize int) {
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

	// max값 판별 함수 
	MaxOfSensorData := func(reg64s []uint16, currentTime time.Time, maxData maxDataStruct) maxDataStruct {
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

	// 모드버스 통신 값을 읽고 interval 마다 Storage buffer에 데이터를 저장시켜주는 역할
	ReadModbus := func (index int, device *modbus.ModbusClient,addr, quantity uint16, wg *sync.WaitGroup, count, batchsize int, ticker *time.Ticker) {
		dvcid := fmt.Sprintf("D0000000%v", index + 1)
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
					if _, exist := Storage[dvcid]; !exist {
						// 주소를 저장 (새롭게 생성)
						Storage[dvcid] = &copyData
					} else {
						// 저장된 주소값에 역참조로 데이터를 덮어씌움 (업데이트)
						*(Storage[dvcid]) = copyData
					}
					mu.Unlock()


					cnt++

					maxData.maxValues = make([]uint16, 0)
					maxData.maxTimestamp = time.Time{}
				}
			}
		}()
		// 0.5초마다 max값 판별
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

				// id 통일은 아래
				err := device.SetUnitId(uint8(1))
				
				// slave id 다른건 아래
				// err := device.SetUnitId(uint8(index + 1))

				if err != nil {
					fmt.Println(err)
					return
				}

				reg64s, err = device.ReadRegisters(addr - 1,quantity,modbus.HOLDING_REGISTER)
				if err != nil {
					fmt.Println(err)
					return
				}

				currentTime := time.Now()
				var maxDatas = MaxOfSensorData(reg64s, currentTime, maxData)
				sendMaxDataChan <- maxDatas
				fieldName := "DV00" + strconv.Itoa(index)
				mu.Lock()
				testCount[fieldName]++
				mu.Unlock()
	
				MaxCount++

				// 0.05초 대기
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	app.Post("/", func(c fiber.Ctx) error {
		type readBody struct {
			Dvc_len int
		}

		var result readBody

		err := c.Bind().Body(&result)
		
		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		Start("192.168.100.83", result.Dvc_len)
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
			go SensorBatchInsert(result.Batchsize)
			for index, device := range Device_Client {
				ticker := time.NewTicker(300 * time.Millisecond)
				wg.Add(1)
				go ReadModbus(index, device, 1300, 125, &wg ,result.Count, result.Batchsize, ticker)
			}
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
		result := DisconnectModbus("192.168.100.108")
		return c.SendString(result)
	})

	app.Get("/log", func(c fiber.Ctx) error {
		for key,  val := range Storage{
			fmt.Println(key)
			fmt.Println(*&val.maxTimestamp)
			fmt.Println(*&val.maxValues)
		}
		return c.SendString("Q")
	})

	log.Fatal(app.Listen(":3000"))
}
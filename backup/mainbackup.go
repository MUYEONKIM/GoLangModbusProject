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

	// "strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	"github.com/simonvetter/modbus"
)

var err error
var (
	Device_Client []*modbus.ModbusClient
	client *modbus.ModbusClient
	timestamp time.Time
	mu sync.Mutex
	wg sync.WaitGroup
)

var stopReadChan = make(chan bool)
var dataReadyChan = make(chan struct{})
var sendMaxDataChan = make(chan SensorData, 1)
var saveSnsrChan = make(chan SensorData, 1)


type snsrCon struct {
    Snsr_nm string // 구조체 필드는 대문자로 시작해야 외부에서 접근 가능
    Address int 
}

type result struct {
	Result []snsrCon 
}

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

func ConnectModbus(ip string, port string) *modbus.ModbusClient{
	client, err = modbus.NewClient(&modbus.ClientConfiguration{
		// URL: "tcp://" + ip + ":" + "502",
		URL: "tcp://" + ip + ":" + port,
		Speed : 1000,
		Timeout: 1 * time.Second,
		
	})
	err = client.Open()
	if err != nil {
		fmt.Println(err)
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

	snsrDataSave := func(sizeData batchType) {
		var values []interface{}
		var addQueryParameter []string
		basequery := "INSERT INTO t_storage_test (dvc_id, address, value, timestamp) VALUES "

		// 모든 데이터를 한 번의 INSERT로 처리
		for i, value := range sizeData.snsrValue {
			dvcIndex := i / 125
			dvcid := sizeData.dvcId[dvcIndex]
			timestamp := sizeData.snsrTimeStamp[dvcIndex]
			
			address := 1300 + (i % 125)
			addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?)")
			values = append(values, 
				dvcid,      
				address,                     
				value,   // value를 직접 사용
				timestamp,
			)
		}

		// 한 번의 SQL 실행으로 모든 데이터 삽입
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

	SaveIntevalSnsr := func(dvcid string, reg64s []uint16, currentTime time.Time, maxData SensorData) SensorData {
		if len(reg64s) == 0 {
			log.Fatal("No data")
			fmt.Println("No data")
		}

		if len(maxData.maxValues) == 0 {
			maxData.dvcid = dvcid
			maxData.maxValues = reg64s
			maxData.maxTimestamp = currentTime
		}
		// maxValues가 담기는 이유는, 슬라이스라서 그렇다. 슬라이스는 pass by value로 되더라도, 가리키는 배열의 주소가 복사되므로, 자동으로 pass by pointer가 되는데
		// maxTimestamp는 time.Time이라서 pass by value로 되어서, 값이 복사되어서 들어가게 된다. 따라서 기존의 값이 변경되는 것이 아님 

		for i, value := range reg64s {
			if maxData.maxValues[i] < value{
				maxData.dvcid = dvcid
				maxData.maxValues[i] = value
				maxData.maxTimestamp = currentTime
			}
		}

		return maxData
	}

	SensorBatchInsert := func(batchsize int) {
		batchData := batchType{
			snsrValue: make([]uint16, 0),
			snsrTimeStamp : make([]time.Time, 0),
			dvcId: make([]string, 0),
		}

		ticker := time.NewTicker(300 * time.Millisecond)

		cnt := 0
		defer ticker.Stop()
		for {
			select {
			case <- stopReadChan:
				fmt.Println("Stop save")
				return
			case snsrData := <- saveSnsrChan:
				mu.Lock()
				cnt++
				batchData.snsrValue = append(batchData.snsrValue, snsrData.maxValues... )
				batchData.snsrTimeStamp = append(batchData.snsrTimeStamp, snsrData.maxTimestamp)
				batchData.dvcId = append(batchData.dvcId, snsrData.dvcid)
				mu.Unlock()
			case <-ticker.C:
				mu.Lock()
				batchLength := len(batchData.snsrValue)
				if batchLength > 0 {
					if batchLength >= batchsize {

						quot := batchLength / batchsize
						rem := batchLength % batchsize


						for i := 0; i < quot; i++ {
							startIdx := i * batchsize
							endIdx := (i + 1) * batchsize
							
							// dvcId와 timestamp 인덱스 계산
							dvcStartIdx := startIdx / 125
							dvcEndIdx := (endIdx - 1) / 125 + 1
							
							sizeData := batchType{
								snsrValue:    batchData.snsrValue[startIdx:endIdx],
								snsrTimeStamp: batchData.snsrTimeStamp[dvcStartIdx:dvcEndIdx],
								dvcId:    batchData.dvcId[dvcStartIdx:dvcEndIdx],
							}
							snsrDataSave(sizeData)
						}

						if rem != 0 {
							startIdx := quot * batchsize
							// 남은 데이터에 대한 dvcId와 timestamp 인덱스 계산
							dvcStartIdx := startIdx / 125
							dvcEndIdx := (batchLength - 1) / 125 + 1
							
							sizeData := batchType{
								snsrValue:    batchData.snsrValue[startIdx:],
								snsrTimeStamp: batchData.snsrTimeStamp[dvcStartIdx:dvcEndIdx],
								dvcId:    batchData.dvcId[dvcStartIdx:dvcEndIdx],
							}
							snsrDataSave(sizeData)
							}
							
						batchData = batchType{
							snsrValue: make([]uint16, 0),
							snsrTimeStamp : make([]time.Time, 0),
							dvcId:     make([]string, 0),
						}
					} else {
						dvcEndIdx := (batchLength - 1) / 125 + 1
            
						sizeData := batchType{
							snsrValue:    batchData.snsrValue,
							snsrTimeStamp: batchData.snsrTimeStamp[:dvcEndIdx],
							dvcId:    batchData.dvcId[:dvcEndIdx],
						}
						snsrDataSave(sizeData)

						batchData = batchType{
							snsrValue: make([]uint16, 0),
							snsrTimeStamp : make([]time.Time, 0),
							dvcId:     make([]string, 0),
						}
					}
				}
				mu.Unlock()
			}
		}
	}
	

	ReadModbus := func (index int, device *modbus.ModbusClient,addr, quantity uint16, wg *sync.WaitGroup, count, batchsize int, ticker *time.Ticker) {
		var maxData = SensorData{}
		defer wg.Done()
		defer ticker.Stop()
		MaxCount := 0
		dvcid := fmt.Sprintf("D0000000%v", index + 1)

		cnt := 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
					// 지금 여기서 maxData에 담기는게, 마지막 인덱스의 maxdata로 저장이 되는건지 테스트 필요
				case result := <- sendMaxDataChan:
					maxData = result
				case <- ticker.C:
					cnt++
					fmt.Println(dvcid, " : ", cnt)
					saveSnsrChan <- maxData
					maxData.maxValues = make([]uint16, len(maxData.maxValues))
					maxData.maxTimestamp = time.Time{}
				}
			}
		}()
		for {
			select {
			case <- stopReadChan:
				fmt.Println("Stop read")
				return
			default:
				if MaxCount == count {
					mu.Lock()
					fmt.Printf("%+v \n", testCount)
					mu.Unlock()
					return
				}
				var reg64s []uint16

				err := device.SetUnitId(uint8(index + 1))
				if err != nil {
					fmt.Println(err)
					return
				}

				reg64s, err = device.ReadRegisters(addr - 1,quantity,modbus.HOLDING_REGISTER)
				if err != nil {
					fmt.Println(err)
					return
				}
				// fmt.Println(MaxCount)

				currentTime := time.Now()

				var maxDatas = SaveIntevalSnsr(dvcid, reg64s, currentTime, maxData)
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

	ReadSnsrConf := func(addr int) []snsrCon{
		rows, err := db.Query("SELECT snsr_nm, address FROM t_snsr_conf WHERE address >= ? && dvc_cd = 'D000000004'", addr )
		var result []snsrCon
		var snsr_nm string
		var address int

		defer rows.Close()
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		for rows.Next() {
			err := rows.Scan(&snsr_nm, &address)

			if err != nil {
				fmt.Println(err)
				log.Fatal(err)
			}

			result = append(result, snsrCon{ Snsr_nm: snsr_nm, Address: address})
		}
		return result
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

	app.Get("/sql", func(c fiber.Ctx) error {
		ReadSnsrConf(1300)
		return nil
	})

	app.Post("/test", func(c fiber.Ctx) error {
		type testtype struct {
			Dvc_ids []string  
			Address []int 
		} 

		result := testtype{}


		c.Bind().Body(&result)
		fmt.Println(result)

		// var result testtype

		// var result2 map[string]interface{}

		// err := c.Bind().Body(&result)
		// if err != nil {
		// 	fmt.Println(err)
		// 	return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		// }
		// fmt.Println(result.Address)
		// fmt.Println(result.Dvc_ids)

		// err := c.Bind().Body(&result2)
		// if err != nil {
		// 	fmt.Println(err)
		// 	return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		// }

		// fmt.Println(result2["address"])
		// fmt.Printf("%+v",result2)

		// if err := c.(&result); err != nil {
			// return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		// }

		testresult := "ㅂㅂㅂㅂ"

		return c.SendString(testresult)
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
		stopReadChan = make(chan bool, len())
		// saveSnsrChan = make(chan SensorData, 1)


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


	log.Fatal(app.Listen(":3000"))
}
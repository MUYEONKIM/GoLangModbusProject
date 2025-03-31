package main

import (
	"context"
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
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/simonvetter/modbus"
)



type SensorData struct {
	dvcid string
	maxValues []uint16
	maxTimestamp time.Time
}

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
	readWaitGroup sync.WaitGroup
	SqlDeviceData SqlDeviceDataType

	// map관리
	// Storage = make(map[string]*maxDataStruct)
	Storage =cmap.New[*maxDataStruct]()
	shardStorage  = cmap.New[cmap.ConcurrentMap[string, *maxDataStruct]]()

	// Device_Clients = make(map[string]*Device)
	Device_Clients = cmap.New[*Device]()
	shardMap = cmap.New[*sql.DB]()
	// 채널관리
	DeviceReadChan = make(chan bool)
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
				switch Device.com {
				case 0:
					err := Device.clientInfo.Open()

					if err != nil {
						fmt.Println(key, "error : ", err)
						Device.errorCount++
						continue
					}

					Device.com = 2
					Device.errorCount = 0
					Device.sucessCount++
					
					readWaitGroup.Add(1)

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
func Start(SqlDeviceDatas []SqlDeviceDataType, Batchsize int) error {
	if len(SqlDeviceDatas) != 0 {
		var connectWg sync.WaitGroup
		for _,  SqlDeviceData := range SqlDeviceDatas {
			connectWg.Add(1)
			go func(){
				var client *modbus.ModbusClient

				defer connectWg.Done()
				if _, exists := Device_Clients.Get(SqlDeviceData.dvc_id); !exists {
	
					client, err = modbus.NewClient(&modbus.ClientConfiguration{
						// URL: "tcp://" + "192.168.100.108" + ":" + "502",
						URL: "tcp://" + SqlDeviceData.dvc_ip + ":" + SqlDeviceData.dvc_port,
						Speed : 1000,
						Timeout: 1 * time.Second,
					})
					client.SetUnitId(uint8(SqlDeviceData.dvc_slaveid))
					client.SetEncoding(modbus.BIG_ENDIAN, modbus.WordOrder(modbus.LOW_WORD_FIRST))

					if err != nil {
						fmt.Println("에러 : " ,err)
					}

					// // id 통일은 아래
					// // slave id 다른건 아래
					// err := device.SetUnitId(uint8(index + 1))
					Device_Clients.Set(SqlDeviceData.dvc_id, &Device{
						clientInfo: client,
						dvcInfo: SqlDeviceData,
					})
				}

			}()
			connectWg.Wait()
		}

		go ConnectModbus()
		go SensorBatchInsert(Batchsize)
	}
	return fmt.Errorf("최소.")
}


func DisconnectModbus(dvc_ids []string) error {
	for _, dvc_id := range dvc_ids {
		client, stat := Device_Clients.Get(dvc_id)
		if !stat {
			return fmt.Errorf("%+v 디바이스가 존재하지 않습니다.", dvc_id)
		}
		err = client.clientInfo.Close()
		if err != nil {
			fmt.Println(err)
			return err
		}

		// 1로 변경하여 읽기 및 저장이 안 되도록 함
		client.com = 1
		
	}
	return nil
}

func RestartModbus(dvc_ids []string) error {
	for _, dvc_id := range dvc_ids {
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

				readWaitGroup.Add(1)
				go ReadModbus(client)
			case 2:
				client.clientInfo.Close()
				client.com = 0
				// 어차피 재시작은 connect go routine에서 관리하고 있음
		}
	}
	return nil
}

// max값 판별 함수 

// add Generic Type

func MaxOfSensorData(reg64s []float32, currentTime time.Time, maxData maxDataStruct) maxDataStruct  {
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
	
	// maxValues의 정적 길이 할당 이지만 (sql에서 가져온 데이터의 길이만큼 동적 할당)
	var (
		sendMaxDataChan = make(chan maxDataStruct)
		saveChan = make(chan bool)
		err error
		maxDataWaitGroup sync.WaitGroup
		maxData = maxDataStruct{
			maxValues: make([]float32, 0, device.dvcInfo.quantity),
		}
	)
	
	DvcId := device.dvcInfo.dvc_id
	shardKey := device.dvcInfo.shard_key

	readTicker := time.NewTicker(300000 * time.Microsecond)
	shardBuffer , _ := shardStorage.Get(shardKey)

	ctx, cancel := context.WithCancel(context.Background())

	maxDataWaitGroup.Add(1)

	defer func() {
		cancel()
		readTicker.Stop()
		shardBuffer.Remove(DvcId)
		readWaitGroup.Done()
		close(saveChan)
		close(sendMaxDataChan)
		fmt.Printf("%+v 종료 \n", device.dvcInfo.dvc_id)
	}()

	go func() {
		defer maxDataWaitGroup.Done()
		for {
			select {
			case <- ctx.Done():
				return
			case <- saveChan:
				return
			case result := <- sendMaxDataChan:
				maxData = result
			case <- readTicker.C:
				copyData := maxData

				if _, exist := shardBuffer.Get(DvcId); !exist {
					shardBuffer.Set(DvcId, &copyData)
				}	else {
					storageData, _ := shardBuffer.Get(DvcId)
					*storageData = copyData
				}


				maxData.maxValues = make([]float32, device.dvcInfo.quantity)
				maxData.maxTimestamp = time.Time{}
			}
		}
	}()
	// 0.05초마다 max값 판별
	for {
		select {
		case <- ctx.Done():
			maxDataWaitGroup.Wait()
			return
		default:
	// 상태값 관리
			isOpen, _ := Device_Clients.Get(device.dvcInfo.dvc_id)
			switch isOpen.com {
			case 1:
				return
			case 2:
				sensorConversionData := make([]float32, device.dvcInfo.quantity)
				currentTime := time.Now()

				switch device.dvcInfo.dvc_type {
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
					saveChan <- true
					device.com = 0
					fmt.Println("ReadRegister : ",device.dvcInfo.dvc_id , err)
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
}

func SensorBatchInsert(batchsize int) {
	// var shardDBSaveWaitGroup sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<- DeviceReadChan
		cancel()
	}()
	// 여기서 shard key가 구분이 되어야 함
	// 이보다 상위로 가게되면 그만큼의 go routine이 생겨버림
		for key, value := range shardMap.Items() {
			basequery := "INSERT INTO storage (dvc_id, address, value, timestamp, datasavedtime) VALUES "
			db := value
			// shardDBSaveWaitGroup.Add(1)
			go func(db *sql.DB, shardKey string){
				// defer shardDBSaveWaitGroup.Done()

				
				for {
					select {
					case <-ctx.Done():
						fmt.Println("Stop Save for shard key:", shardKey)
						return 
					default:
						// sleep를 처음에 주는 이유는, 마지막에 존재할 시, 처음에 값을 storage에 저장 시키기도 전에 실행을 먼저 시켜서 첫번째 maxdata가 유실이 되버림
						// 그래서 처음에 0.3초를 먼저 기다린 후에 저장 로직을 하도록 해야 첫번재 maxdata가 저장이 됨
						time.Sleep(300000 * time.Microsecond)
						shardDevideStorage, _ := shardStorage.Get(shardKey)
						StorageLen := shardDevideStorage.Count()


						if StorageLen > 0 {
							currentTime := time.Now()
							var batchwg sync.WaitGroup
							
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
									batchwg.Add(1)
									go func(startIdx int, endIdx int) {
										defer batchwg.Done()

										totalItems := 0
										for i := startIdx; i < endIdx; i++ {
											key := deviceKeys[i]
											device , _ := Device_Clients.Get(key)
											totalItems += device.dvcInfo.quantity
										}

										
										// 미리 용량 할당
										addQueryParameter := make([]string, 0, totalItems)
										values := make([]interface{}, 0, totalItems * 5) // 각 항목당 5개 값
										
										for i := startIdx; i < endIdx; i++ {
											key := deviceKeys[i]
											value, _ := shardDevideStorage.Get(key)
											device , _ := Device_Clients.Get(key)
											timeStamp := value.maxTimestamp
											for valIdx , snsrValue := range value.maxValues {
												addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?, ?)")
												address := device.dvcInfo.dvc_remap + valIdx
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
											tx, err := db.Begin()
											if err != nil {
												log.Printf("트랜잭션 시작 오류: %v", err)
												return
											}
											
											_, err = tx.Exec(sql, values...)
											if err != nil {
												tx.Rollback() // 오류 발생 시 롤백
												log.Printf("배치 SQL 삽입 오류: %v", err)
												return
											}
											
											err = tx.Commit() // 트랜잭션 커밋
											if err != nil {
												log.Printf("트랜잭션 커밋 오류: %v", err)
												return
											}

											addQueryParameter = nil
											values = nil
										}
									}(startIdx, endIdx)
									// 현재 배치에 속한 디바이스만 처리
									

									// 여기까지?
								}
								batchwg.Wait()
							} else {
								totalItems := 0
								for key, _ := range shardDevideStorage.Items() {
									device , _ := Device_Clients.Get(key)
									totalItems += device.dvcInfo.quantity
								}
								
								// 미리 용량 할당
								addQueryParameter := make([]string, 0, totalItems)
								values := make([]interface{}, 0, totalItems * 5) 
						
								for key, value := range shardDevideStorage.Items() {
									timeStamp := value.maxTimestamp
									device , _ := Device_Clients.Get(key)

									for i, snsrValue := range value.maxValues {
										addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?, ?)")
										address := device.dvcInfo.dvc_remap + i
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

									tx, err := db.Begin()
									if err != nil {
										log.Printf("트랜잭션 시작 오류: %v", err)
										continue
									}
									
									_, err = tx.Exec(sql, values...)
									if err != nil {
										tx.Rollback()
										log.Printf("SQL 삽입 오류: %v", err)
										continue
									}
									
									err = tx.Commit() 
									if err != nil {
										log.Printf("트랜잭션 커밋 오류: %v", err)
										continue
									}
									
									addQueryParameter = nil
									values = nil
								}
							}
				
						}
					}
				}
		}(db, key)
	}
	// shardDBSaveWaitGroup.Wait()
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

	SHARD_ONE_DB_HOST := os.Getenv("SHARD_ONE_DB_HOST")
	SHARD_ONE_DB_PORT := os.Getenv("SHARD_ONE_DB_PORT")
	SHARD_ONE_DB_USER := os.Getenv("SHARD_ONE_DB_USER")
	SHARD_ONE_DB_PASSWORD := os.Getenv("SHARD_ONE_DB_PASSWORD")
	SHARD_ONE_DB_DATABASE := os.Getenv("SHARD_ONE_DB_DATABASE")

	// SHARD_TWO_DB_HOST := os.Getenv("SHARD_TWO_DB_HOST")
	// SHARD_TWO_DB_PORT := os.Getenv("SHARD_TWO_DB_PORT")
	// SHARD_TWO_DB_USER := os.Getenv("SHARD_TWO_DB_USER")
	// SHARD_TWO_DB_PASSWORD := os.Getenv("SHARD_TWO_DB_PASSWORD")
	// SHARD_TWO_DB_DATABASE := os.Getenv("SHARD_TWO_DB_DATABASE")

	dsns := []string{
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE),
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", SHARD_ONE_DB_USER, SHARD_ONE_DB_PASSWORD, SHARD_ONE_DB_HOST, SHARD_ONE_DB_PORT, SHARD_ONE_DB_DATABASE),
		// fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", SHARD_TWO_DB_USER, SHARD_TWO_DB_PASSWORD, SHARD_TWO_DB_HOST, SHARD_TWO_DB_PORT, SHARD_TWO_DB_DATABASE),
	}

	for i, dsn := range dsns {
		Storage1 := cmap.New[*maxDataStruct]()

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatal(err)
		}

		db.SetMaxOpenConns(30) // 최대 연결 수 제한
		db.SetMaxIdleConns(30)  // 유휴 연결 수 제한

		shardKey := "shard" + strconv.Itoa(i + 1)

		if err = db.Ping(); err != nil {
			log.Fatal(err)
		}

		if _, exist := shardMap.Get(shardKey); !exist {
			shardMap.Set(shardKey, db)
			shardStorage.Set(shardKey, Storage1)
		}
	}
	app := fiber.New()
	

	app.Post("/connectall", func(c fiber.Ctx) error {
		type reqBody struct {
			Batchsize int
		}

		var SqlDeviceDatas []SqlDeviceDataType
		var result reqBody

		err := c.Bind().Body(&result)
		
		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		db, status := shardMap.Get("shard1")

		if !status {
			return c.Status(fiber.StatusBadRequest).SendString("MAIN DB connect error.")
		}

		rows, err := db.Query("SELECT ds.dvc_id, ds.dvc_type, ds.cmpn_cd, ds.dvc_ip, ds.dvc_port, ds.dvc_remap, ds.quantity, ds.dvc_interval, ds.dvc_timeout, ds.dvc_slaveid, ds.protocol_type, cinfo.shard_key FROM t_dvc_save ds LEFT JOIN companyinfo cinfo ON ds.cmpn_cd = cinfo.cmpn_cd")
		if err != nil {
			fmt.Println(err)
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


		err = Start(SqlDeviceDatas, result.Batchsize)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		return c.SendString("성공적으로 연결되었습니다.")
	})

	app.Get("/readstop", func(c fiber.Ctx) error {
		close(saveSnsrChan)
		// close(sendMaxDataChan)
		close(DeviceReadChan)
		// wg.Wait()
		return c.SendString("읽기를 멈춥니다")
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

		err = DisconnectModbus(result.Dvc_Id)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
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

		err = RestartModbus(result.Dvc_Id)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		return c.Status(200).SendString("재시작 되었습니다.")
	})

	app.Get("/log", func(c fiber.Ctx) error {
		// fmt.Println(len(Device_Clients))
		// fmt.Printf("%+v \n", Storage)
		// for key,  val := range Storage{
		// 	fmt.Println(key)
		// 	fmt.Println(*&val.maxTimestamp)
		// 	fmt.Println(*&val.maxValues)
		// }

		for key, val := range Device_Clients.Items() {
			fmt.Printf("%v : %+v \n",key, val)
		}

		return c.SendString("Q")
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

		db, status := shardMap.Get("shard1")

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
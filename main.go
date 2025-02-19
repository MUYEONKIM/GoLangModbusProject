package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"
	"github.com/simonvetter/modbus"
)

var stopReadChan = make(chan bool)
var err error
var (
	Device_Client []*modbus.ModbusClient
	client *modbus.ModbusClient
	timestamp time.Time
	mu sync.Mutex
)

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

// type snsrConf struct {
//     SnsrCd       string  `db:"snsr_cd"`
//     DvcCd        string  `db:"dvc_cd"`
//     SnsrNm       string  `db:"snsr_nm"`
//     CtYScale     float64 `db:"ct_y_scale"`
//     CtNScale     float64 `db:"ct_n_scale"`
//     Address      int     `db:"address"`
//     UseYn        string  `db:"use_yn"`
//     AutoIncrement int    `db:"auto_increment"`
// }

type snsrCon struct {
    Snsr_nm string // 구조체 필드는 대문자로 시작해야 외부에서 접근 가능
    Address int 
}

type result struct {
	Result []snsrCon 
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



	SaveSnsrConf := func(index int, reg64s []uint16, batchsize int, current_time time.Time) {
		// 1. Bulk insert + prepare statement 방식 (데이터 한번에 insert)
		// t0 := time.Now()
		// if len(reg64s) == 0 {
		// 	fmt.Println("No data")
		// 	return
		// }

		// var values []interface{}
		// var addQueryParameter []string
		// basequery := "INSERT INTO t_storage_test (address, value) VALUES "

		// for i, value := range reg64s {
		// 	address := 1300 + i
		// 	addQueryParameter = append(addQueryParameter, "(?, ?)")
		// 	values = append(values, address, value)	
		// }

		// sql := basequery + strings.Join(addQueryParameter, ", ") 
		// stmt, err := db.Prepare(sql)
		// if err != nil {
		// 	log.Fatal(err)
		// }
				
		// _ , err = stmt.Exec(values...)
		// if err != nil {
		// 	fmt.Println(err)
		// 	log.Fatal(err)
		// }

		// values = make([]interface{}, 0)
		// addQueryParameter = make([]string, 0)
		// defer stmt.Close()
		// t1 := time.Now()
		// defer fmt.Printf("%v per second. \n", float64(len(reg64s))/t1.Sub(t0).Seconds())

		// 1.5 batch 적용방식
		// t0 := time.Now()



		var values []interface{}
		var addQueryParameter []string

		dvc_id := fmt.Sprintf("D0000000%v", index + 1)

		basequery := "INSERT INTO t_storage_test (dvc_id, address, value, timestamp) VALUES "

		for i, value := range reg64s {
			address := 1300 + i
			addQueryParameter = append(addQueryParameter, "(?, ?, ?, ?)")
			values = append(values, dvc_id, address, value, current_time)	

			if (i+1) % batchsize == 0 {
				sql := basequery + strings.Join(addQueryParameter, ", ") 
				stmt, err := db.Prepare(sql)
				if err != nil {
					log.Fatal(err)
				}
				
				_ , err = stmt.Exec(values...)
				if err != nil {
					fmt.Println(err)
					log.Fatal(err)
					stmt.Close()
					return
				}

				values = make([]interface{}, 0)
				addQueryParameter = make([]string, 0)
				stmt.Close()
			}
		}

		if len(values) > 0 {
			sql := basequery + strings.Join(addQueryParameter, ", ")
			stmt, err := db.Prepare(sql)
			if err != nil {
				fmt.Println(err)
				log.Fatal(err)
				stmt.Close()
			}	

			_, err = stmt.Exec(values...)
			if err != nil {
				fmt.Println(err)
				log.Fatal(err)
				stmt.Close()
			}

			stmt.Close() 
		}

		// t1 := time.Now()
		// defer fmt.Printf("%v per second. \n", float64(len(reg64s))/t1.Sub(t0).Seconds())

		
		// 2. 그냥 센서 하나마다 insert 반복문 돌리는거
		// t0 := time.Now()
		// for i, value := range reg64s {
		// 	_, err := db.Exec("INSERT INTO t_storage_test (address, value) VALUES (?, ?)", 1300 + i, value)	
		// 	if err != nil {
		// 		fmt.Println(err)
		// 		log.Fatal(err)
		// 	}
		// }
		// t1 := time.Now()
		// defer fmt.Printf("%v per second. \n", float64(len(reg64s))/t1.Sub(t0).Seconds())


		// 3. prepare sql statement 사용
		// t0 := time.Now()

		// stmt, err := db.Prepare("INSERT INTO t_storage_test (address, value) VALUES (?, ?)")
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// defer stmt.Close() // 함수가 끝날 때 닫기
	
		// // Bulk insert
		// for i, value := range reg64s {
		// 	address := 1300 + i
	
		// 	// Execute the prepared statement
		// 	_, err := stmt.Exec(address, value)
		// 	if err != nil {
		// 		fmt.Println(err)
		// 		log.Fatal(err)
		// 	}
		// }
		// t1 := time.Now()
		// defer fmt.Printf("%v per second. \n", float64(len(reg64s))/t1.Sub(t0).Seconds())

		}

		SaveIntevalSnsr := func(index int, reg64s []uint16, batchsize int, current_time time.Time, intervalCount int) {
			if len(reg64s) == 0 {
				fmt.Println("No data")
				return
			}
			
			var maxValues []uint16
			var maxTimestamp time.Time

			if len(maxValues) == 0 {
				maxValues = reg64s
			}

			for i, value := range reg64s {
				if maxValues[i] < value{
					maxValues[i] = value
					maxTimestamp = current_time
				}
			}
			if intervalCount % 6 == 0 {
				// fmt.Printf("%v 저장되었습니다.",maxTimestamp)
				SaveSnsrConf(index, maxValues, batchsize, maxTimestamp)
				maxValues = []uint16{}
				intervalCount = 0
			}

			intervalCount++
		}
	

	ReadModbus := func (index int, device *modbus.ModbusClient,addr, quantity uint16, wg *sync.WaitGroup, count int, batchsize int) {
		defer wg.Done()
		MaxCount := 0
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
			var intervalCount int

				device.SetUnitId(uint8(index + 1))
				reg64s, err = device.ReadRegisters(addr - 1,quantity,modbus.HOLDING_REGISTER)
				if err != nil {
					fmt.Println(err)
					return
				}
				current_time := time.Now()
				// fmt.Printf("디바이스%v : %+v \n",index + 1, reg64s)
				// SaveSnsrConf(index, reg64s, batchsize, current_time)
				SaveIntevalSnsr(index, reg64s, batchsize, current_time, intervalCount)
				fieldName := "DV00" + strconv.Itoa(index)
				mu.Lock()
				testCount[fieldName]++
				mu.Unlock()
	
				MaxCount++
				// fmt.Println(index, MaxCount)
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

		stopReadChan = make(chan bool)

		var wg sync.WaitGroup
		if len(Device_Client) != 0 {
			for index, device := range Device_Client {
				wg.Add(1)
				go ReadModbus(index, device, 1300, 125, &wg ,result.Count, result.Batchsize)
			}
		} else {
			log.Fatal("연결된 디바이스가 없습니다.")
			return c.SendString("연결된 디바이스가 없습니다")
		}
		return nil
	})

	app.Get("/readstop", func(c fiber.Ctx) error {
		close(stopReadChan)
		return c.SendString("읽기를 멈춥니다")
	})

	app.Get("/disconnect", func(c fiber.Ctx) error {
		result := DisconnectModbus("192.168.100.108")
		return c.SendString(result)
	})


	log.Fatal(app.Listen(":3000"))
}
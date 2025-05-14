package main

import (
	"fmt"
	"log"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"os"

	"github.com/gofiber/fiber/v3"
	"github.com/joho/godotenv"

	"GOPROJECT/db/mysql"
	"GOPROJECT/db/nosql"
	"GOPROJECT/protocol/modbus"
	"GOPROJECT/protocol/mqtt"
	"GOPROJECT/router"
	"GOPROJECT/utils"
)

// 전역 변수 관리
var (
	err error
	// map관리
	DEVICE_CONFIG string
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("ENV 파일이 잘못되었습니다.")
	}
	DEVICE_CONFIG = os.Getenv("DEVICE_CONFIG")

	mysqlDB := mysql.MysqlDatabase()

	if err = mysqlDB.Ping(); err != nil {
		utils.HandleError("MYSQL Connection Error : " , err)
	}

	BATCH_SIZE := os.Getenv("BATCH_SIZE")
	batchSize, err := strconv.Atoi(BATCH_SIZE)

	if err != nil {
		utils.HandleError("BATCH_SIZE Error : ", err)
	}
	
	nosql.InitMongoShard()
	modbus.ModbusInit()
	
	go modbus.ConnectModbus()

	switch DEVICE_CONFIG {
	case "0":
		fmt.Println("MQTT 데이터 전송 Config로 실행되었습니다")
		go mqtt.SendMqttMessage()
	case "1":
		fmt.Println("데이터 Sharding 저장 Config로 실행되었습니다")
		go modbus.SensorBatchInsert(batchSize)
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
	WORKER_PORT := os.Getenv("WORKER_PORT")

	app := fiber.New()
	router.Router(app)

	log.Fatal(app.Listen(":" + WORKER_PORT))
}
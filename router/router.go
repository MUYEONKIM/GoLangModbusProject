package router

import (
	"GOPROJECT/protocol/modbus"
	"GOPROJECT/sql"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v3"
)

type reqBody struct{
	Dvc_Id []string
}

func Router(app *fiber.App) {
	app.Post("/connect", func(c fiber.Ctx) error {
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
			sqlResult, err := sql.GetDeviceListOne(v)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
			modbus.Start(sqlResult)
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
			err = modbus.DisconnectModbus(v)

			if err != nil {
				log.Println(err)
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
		}

		return c.Status(200).SendString("종료되었습니다.")
	})

	app.Post("/restart", func(c fiber.Ctx) error {

		var result reqBody
		err := c.Bind().Body(&result)

		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		for _, dvc_id := range result.Dvc_Id {
			err = modbus.RestartModbus(dvc_id)

			if err != nil {
				log.Println(err)
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
		}
		return c.Status(200).SendString("재시작 되었습니다.")
	})

		app.Get("/", func(c fiber.Ctx) error {
		res := fmt.Sprintf("디바이스 연결 개수 : %v", 123)
		return c.Status(200).SendString(res)
	})


	// app.Get("/log", func(c fiber.Ctx) error {
	// 	fmt.Println(Device_Clients.Count())
	// 	// fmt.Printf("%+v \n", Storage)
	// 	// for key,  val := range Storage{
	// 	// 	fmt.Println(key)
	// 	// 	fmt.Println(*&val.maxTimestamp)
	// 	// 	fmt.Println(*&val.maxValues)
	// 	// }

	// 	// for key, val := range Device_Clients.Items() {
	// 	// 	fmt.Printf("%v : %+v \n",key, val)
	// 	// }
	// 	// val ,_ := shardMap.Get("shard1")

	// 	// fmt.Println(val.Stats())
	// 	res := fmt.Sprintf("디바이스 연결 개수 : %v", Device_Clients.Count())
	// 	return c.Status(200).SendString(res)
	// })

	app.Post("/dbinsert", func(c fiber.Ctx) error {
		var result sql.InsertData

		err := c.Bind().Body(&result)

		if err != nil {
			fmt.Println(err)
			return c.Status(fiber.StatusBadRequest).SendString("잘못된 요청입니다.")
		}

		sql.DeviceInsert(result)

		return nil
	})
}
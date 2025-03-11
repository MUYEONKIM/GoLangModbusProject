func Start(Count int, Batchsize int, ip string, length int, port int, db *sql.DB) error {
	if length != 0 {
		for portNum := port; portNum < port + length; portNum++ {
			wg.Add(1)
			go func(portNum int){
				var client *modbus.ModbusClient
				ticker := time.NewTicker(300000 * time.Microsecond)

				defer wg.Done()
				DvcId := fmt.Sprintf("Device%v", portNum)
	
				mu.Lock()
				if _, exists := Device_Clients[DvcId]; !exists {
					Device_Clients[DvcId] = &Device{
						DvcId: DvcId,
						addr : 1300,
						quantity : 125,
						count: Count,
					}
				}
				mu.Unlock()
	
				client, err = modbus.NewClient(&modbus.ClientConfiguration{
					// URL: "tcp://" + ip + strconv.Itoa(IpAddress) + ":" + "502",
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
					fmt.Println(err)
				} 
				
				// 이 부분을 여기로 빼준이유는, 위 ConnectModbus함수를 재사용 하기 위해
				// 다른 connect error가 발생했을 때, device_clients를 새롭게 생성하면 안되서
				// 여기로 따로 빼주었음
				mu.Lock()
				Device_Clients[DvcId].DeviceInfo = client
				Device_Clients[DvcId].Com = 1
				Device_Clients[DvcId].SucessCount++
				Device_Clients[DvcId].ticker = ticker
			
				startReadChan <- *Device_Clients[DvcId]
				mu.Unlock()

			}(portNum)
		}
		go SensorBatchInsert(Batchsize, db)
	}
	return fmt.Errorf("최소.")
}




func Start(Count int, Batchsize int, ip string, length int, port int, db *sql.DB) error {
	if length != 0 {
		endIp := 108 + length
		
		for IpAddress := 108; IpAddress < endIp ; IpAddress++ {
			fmt.Println(IpAddress)
			wg.Add(1)
			// go func(portNum int){
			go func(){
				var client *modbus.ModbusClient
				ticker := time.NewTicker(300000 * time.Microsecond)

				defer wg.Done()
				DvcId := fmt.Sprintf("Device%v", IpAddress)
				// DvcId := fmt.Sprintf("Device%v", portNum)
	
				mu.Lock()
				if _, exists := Device_Clients[DvcId]; !exists {
					Device_Clients[DvcId] = &Device{
						DvcId: DvcId,
						addr : 1300,
						quantity : 125,
						count: Count,
					}
				}
				mu.Unlock()
	
				client, err = modbus.NewClient(&modbus.ClientConfiguration{
					URL: "tcp://" + ip + strconv.Itoa(IpAddress) + ":" + "502",
					// URL: "tcp://" + ip + ":" + strconv.Itoa(portNum),
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
					fmt.Println(err)
				} 

				// 이 부분을 여기로 빼준이유는, 위 ConnectModbus함수를 재사용 하기 위해
				// 다른 connect error가 발생했을 때, device_clients를 새롭게 생성하면 안되서
				// 여기로 따로 빼주었음
				mu.Lock()
				Device_Clients[DvcId].DeviceInfo = client
				Device_Clients[DvcId].Com = 1
				Device_Clients[DvcId].SucessCount++
				Device_Clients[DvcId].ticker = ticker
			
				startReadChan <- *Device_Clients[DvcId]
				mu.Unlock()
			}()
			// }(portNum)
		}
		go SensorBatchInsert(Batchsize, db)
	}
	return nil
}
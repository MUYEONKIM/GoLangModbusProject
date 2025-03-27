func SensorBatchInsert(batchsize int) {
	fmt.Printf("%+v", shardMap.Items())
	
		for key, value := range shardMap.Items() {
		basequery := "INSERT INTO storage (dvc_id, address, value, timestamp, datasavedtime) VALUES "
		db := value
		
		wg.Add(1)
		go func(db *sql.DB){
			for {
				select {
				case <- stopReadChan:
					fmt.Println("Stop Save")
					return 
				default:
					time.Sleep(300000 * time.Microsecond)
					shardDevideStorage, _ := shardStorage.Get(key)
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
											result, err :=  db.Exec(sql, values...)

											if err != nil {
												log.Fatal(result, err, "batch sql insert error 입니다")
											}

										}
									}(startIdx, endIdx)
								}
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
									result, err :=  db.Exec(sql, values...)
									if err != nil {
										log.Fatal(result, err, "sql insert error 입니다")
									}
								}
							}
						}
					}
				}
			}(db)
	}
}
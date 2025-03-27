func SensorBatchInsert(batchsize int) {
	fmt.Printf("%+v", shardMap.Items())
	
	for key, value := range shardMap.Items() {
		basequery := "INSERT INTO storage (dvc_id, address, value, timestamp, datasavedtime) VALUES "
		db := value
		
		wg.Add(1)
		go func(db *sql.DB, shardKey string){
			defer wg.Done() // 고루틴 종료 시 wg 감소
			
			for {
				select {
				case <- stopReadChan:
					fmt.Println("Stop Save")
					return 
				default:
					time.Sleep(300000 * time.Microsecond)
					shardDeviceStorage, ok := shardStorage.Get(shardKey)
					if !ok {
						continue // 에러 처리 추가
					}
					
					StorageLen := shardDeviceStorage.Count()
					if StorageLen == 0 {
						continue // 저장할 데이터가 없으면 다음 반복으로
					}
					
					currentTime := time.Now()
					var batchwg sync.WaitGroup
					
					if StorageLen > batchsize {
						deviceKeys := make([]string, 0, StorageLen)
						for key := range shardDeviceStorage.Items() {
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
							
							batchwg.Add(1)
							go func(startIdx int, endIdx int) {
								defer batchwg.Done()
								
								totalItems := 0
								for i := startIdx; i < endIdx; i++ {
									key := deviceKeys[i]
									device, ok := Device_Clients.Get(key)
									if !ok {
										continue
									}
									totalItems += device.dvcInfo.quantity
								}
								
								// 미리 용량 할당
								addQueryParameter := make([]string, 0, totalItems)
								values := make([]interface{}, 0, totalItems * 5) // 각 항목당 5개 값
								
								for i := startIdx; i < endIdx; i++ {
									key := deviceKeys[i]
									value, ok := shardDeviceStorage.Get(key)
									if !ok {
										continue
									}
									
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
									
									// 명시적으로 트랜잭션 사용
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
									
									// 메모리 정리를 위해 명시적으로 nil 할당
									addQueryParameter = nil
									values = nil
								}
							}(startIdx, endIdx)
						}
						
						// 모든 배치 고루틴이 완료될 때까지 대기
						batchwg.Wait()
						
					} else {
						totalItems := 0
						for key := range shardDeviceStorage.Items() {
							device, ok := Device_Clients.Get(key)
							if !ok {
								continue
							}
							totalItems += device.dvcInfo.quantity
						}
						
						// 미리 용량 할당
						addQueryParameter := make([]string, 0, totalItems)
						values := make([]interface{}, 0, totalItems * 5) 
				
						for key, value := range shardDeviceStorage.Items() {
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
						
						// 전체 데이터 저장
						if len(values) > 0 {
							sql := basequery + strings.Join(addQueryParameter, ", ")
							
							// 명시적으로 트랜잭션 사용
							tx, err := db.Begin()
							if err != nil {
								log.Printf("트랜잭션 시작 오류: %v", err)
								continue
							}
							
							_, err = tx.Exec(sql, values...)
							if err != nil {
								tx.Rollback() // 오류 발생 시 롤백
								log.Printf("SQL 삽입 오류: %v", err)
								continue
							}
							
							err = tx.Commit() // 트랜잭션 커밋
							if err != nil {
								log.Printf("트랜잭션 커밋 오류: %v", err)
								continue
							}
							
							// 메모리 정리를 위해 명시적으로 nil 할당
							addQueryParameter = nil
							values = nil
						}
					}
					
					// 각 반복 후 GC 힌트 (선택적)
					runtime.GC()
				}
			}
		}(db, key) // 올바른 key 전달
	}
}
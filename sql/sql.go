package sql

import (
	"GOPROJECT/db/mysql"
	"fmt"
	"log"
	"strings"
)

type SqlDeviceDataType struct {
	Dvc_id       string
	Dvc_type     string
	Cmpn_cd      int
	Dvc_ip       string
	Dvc_port     string
	Dvc_remap    int
	Quantity      int
	Dvc_interval int
	Dvc_timeout  int
	Dvc_slaveid  int
	Protocol_type string
	Shard_key    string
}

func GetDeviceListOne(dvcId string) (SqlDeviceDataType, error) {
	var SqlDeviceData SqlDeviceDataType
	var query = "SELECT ds.dvc_id, ds.dvc_type, ds.cmpn_cd, ds.dvc_ip, ds.dvc_port, ds.dvc_remap, ds.quantity, ds.dvc_interval, ds.dvc_timeout, ds.dvc_slaveid, ds.protocol_type, cinfo.shard_key FROM t_dvc_save ds LEFT JOIN companyinfo cinfo ON ds.cmpn_cd = cinfo.cmpn_cd where dvc_id = ?"
	mysqlDB := mysql.MysqlDatabase()

	row := mysqlDB.QueryRow(query, dvcId)

	err := row.Scan(            
		&SqlDeviceData.Dvc_id,
		&SqlDeviceData.Dvc_type,
		&SqlDeviceData.Cmpn_cd,
		&SqlDeviceData.Dvc_ip,
		&SqlDeviceData.Dvc_port,
		&SqlDeviceData.Dvc_remap,
		&SqlDeviceData.Quantity,
		&SqlDeviceData.Dvc_interval,
		&SqlDeviceData.Dvc_timeout,
		&SqlDeviceData.Dvc_slaveid,
		&SqlDeviceData.Protocol_type,
		&SqlDeviceData.Shard_key,
	)

	if err != nil {
		log.Println(err)
		return SqlDeviceData, fmt.Errorf("디바이스 정보를 찾을 수 없습니다.")
	}

	fmt.Println(SqlDeviceData)
	return SqlDeviceData, nil

}

func GetDeviceList() ([]SqlDeviceDataType, error) {
	var SqlDeviceDatas []SqlDeviceDataType
	var query = "SELECT ds.dvc_id, ds.dvc_type, ds.cmpn_cd, ds.dvc_ip, ds.dvc_port, ds.dvc_remap, ds.quantity, ds.dvc_interval, ds.dvc_timeout, ds.dvc_slaveid, ds.protocol_type, cinfo.shard_key FROM t_dvc_save ds LEFT JOIN companyinfo cinfo ON ds.cmpn_cd = cinfo.cmpn_cd "
	mysqlDB := mysql.MysqlDatabase()

	rows, err := mysqlDB.Query(query)

	if err != nil {
		log.Println(err)
	}

	for rows.Next() {
		var SqlDeviceData SqlDeviceDataType

		err := rows.Scan(            
			&SqlDeviceData.Dvc_id,
			&SqlDeviceData.Dvc_type,
			&SqlDeviceData.Cmpn_cd,
			&SqlDeviceData.Dvc_ip,
			&SqlDeviceData.Dvc_port,
			&SqlDeviceData.Dvc_remap,
			&SqlDeviceData.Quantity,
			&SqlDeviceData.Dvc_interval,
			&SqlDeviceData.Dvc_timeout,
			&SqlDeviceData.Dvc_slaveid,
			&SqlDeviceData.Protocol_type,
			&SqlDeviceData.Shard_key,
		)

		if err != nil {
			log.Println(err)
			return SqlDeviceDatas, fmt.Errorf("디바이스 정보를 찾을 수 없습니다.")
		}
		SqlDeviceDatas= append(SqlDeviceDatas, SqlDeviceData)
	}

	return SqlDeviceDatas, nil
}

type InsertData struct {
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

func DeviceInsert(result InsertData) {
	var (
		address, quantity int
		addQueryParameter []string
		values []interface{}
	)

	mysqlDB := mysql.MysqlDatabase()
	basequery := "INSERT INTO t_dvc_save (dvc_id, dvc_type, cmpn_cd, dvc_ip, dvc_port, dvc_remap, quantity, dvc_interval, dvc_timeout, dvc_slaveid, protocol_type) VALUES "
	

	
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
		stmt, err := mysqlDB.Prepare(sql)
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
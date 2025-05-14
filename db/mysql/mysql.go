package mysql

import (
	"GOPROJECT/utils"
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	mysqlDB *sql.DB
	once     sync.Once
	initError error
)

func MysqlDatabase() (*sql.DB){
	once.Do(func() {
		MYSQL_DB_USER := os.Getenv("DB_USER")
		MYSQL_DB_PASSWORD := os.Getenv("DB_PASSWORD")
		MYSQL_DB_HOST := os.Getenv("DB_HOST")
		MYSQL_DB_PORT := os.Getenv("DB_PORT")
		MYSQL_DB_DATABASE := os.Getenv("DB_DATABASE")
		
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", MYSQL_DB_USER, MYSQL_DB_PASSWORD, MYSQL_DB_HOST, MYSQL_DB_PORT, MYSQL_DB_DATABASE)

		mysqlDB, initError = sql.Open("mysql", dsn)

		if initError != nil {
			utils.HandleError("mysqlDB open error: ", initError)
			return 
		}
		
		// connection pool 설정 여기 기입
	})

	return mysqlDB
}
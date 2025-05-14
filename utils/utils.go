package utils

import (
	"bufio"
	"fmt"
	"os"
)

func HandleError(message string, err error) {
	if err != nil {
		fmt.Printf("오류: %s\n%v\n", message, err)
		fmt.Println("엔터키를 누르면 프로그램이 종료됩니다...")
	} else {
		fmt.Printf("오류: %s\n", message)
		fmt.Println("엔터키를 누르면 프로그램이 종료됩니다...")
	}

	// 사용자 입력 기다리기
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n') // 엔터 키가 눌릴 때까지 대기

	// 프로그램 종료
	os.Exit(1)
}
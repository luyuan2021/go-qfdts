package mpkg

import "fmt"

func AllRowsCount(ip,port,user,passwd string,t int) (ret int){
	defer CloseDB()
	_ = ConnectDB(ip, port, user, passwd)
	GetDB()
	tablecount := Process(t)
	result:=0
	for _,v :=range tablecount {
		result=result+v
	}
	PrintLog(fmt.Sprintf("[Info]: All db rows count is %v",result))
	return result
}

package mpkg

import (
	"fmt"
	"strconv"
)

//func MyLoader(ip, port, userName, password string, loadthread string, dumploglevel int) (err error) {
//	cmd := "myloader --user " + userName + " --password " + password + " -h " + ip + " -P " + port + " --directory " + DumpfileDir + " --overwrite-tables" + " --verbose " + strconv.Itoa(dumploglevel) + " --threads " + loadthread
//	fmt.Println(cmd)
//	out := string(Cmd(cmd,true))
//	fmt.Println(out)
//	if err != nil {
//		// 打印输出内容 退出应用程序 defer函数不会执行
//		// log.Fatal()与os.Exit()相比多了打印输出内容
//		log.Fatalf("dumper数据异常,code : %s", err)
//	}
//	return
//}
func MyLoader(ip, port, userName, password string, loadthread string, dumploglevel int, dumpfile string) (err error) {
	cmdArgs := []string{"--user", userName, "--password", password, "-h", ip, "-P", port, "--directory", dumpfile, "--overwrite-tables", "--verbose", strconv.Itoa(dumploglevel), "--threads", loadthread}
	cmd := "mydumper"
	//cmd := "WQload"
	for _, v := range cmdArgs {
		cmd = cmd + " " + v
	}
	PrintLog(cmd)
	ret, err := runCmd("myloader", cmdArgs)
	if err != nil {
		return err
	}
	fmt.Println(ret)
	return
}

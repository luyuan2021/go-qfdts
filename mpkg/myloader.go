package mpkg

import (
	"Myqfdtsproject/jindutiao"
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
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
func MyLoader(srcconn map[string]string,dstip, dstport, dstUser, dspPasswd string, loadthread string, dumploglevel int, dumpfile string,tablecountThread int ) (err error) {
	cmdArgs := []string{"--user", dstUser, "--password", dspPasswd, "-h", dstip, "-P", dstport, "--directory", dumpfile, "--overwrite-tables", "--verbose", strconv.Itoa(dumploglevel), "--threads", loadthread}
	cmd := _load
	//cmd := "WQload"
	for _, v := range cmdArgs {
		cmd = cmd + " " + v
	}
	PrintLog(cmd)

	var wait sync.WaitGroup
	ctxinsert, stop := context.WithCancel(context.Background())
	wait.Add(1)

	go func() {
		defer wait.Done()
		var all_rows int
		func() {
			if dballrows != 0{
				all_rows=dballrows
			}else{
				all_rows = AllRowsCount(srcconn["ip"],srcconn["port"],srcconn["user"],srcconn["passwd"],tablecountThread)
			}
			// get all srcdb rows
		}() // 统计所有数据的行数

		//统计当前的一个初始的innodb_rows_read的值
		initinsert := mysqlstatus(dstip, dstport, dstUser, dspPasswd, "row_insert")
		canalctxinsert(ctxinsert, dstip, dstport, dstUser, dspPasswd, initinsert, all_rows)
	}()

	ok := doload(_load, cmdArgs)
	if ok != nil {
		log.Fatal(err)
	}
	stop()
	wait.Wait()
	return nil
}

func doload(cmd string, args []string) (err error) {
	ret, err := runCmd(cmd, args)
	if err != nil {
		return err
	}
	fmt.Println(ret)
	return nil
}

func canalctxinsert(ctx context.Context, dbip, dbport, dbuserName, dbpassword string, initinsert, allrows int) {
	for {
		select {
		case <-ctx.Done():
			Color(100, "Import data complete...")
			return
		default:
			flag := "Importing data..."
			nowinsert := mysqlstatus(dbip, dbport, dbuserName, dbpassword, "row_insert")
			progress_rate := (nowinsert - initinsert) * 100 / allrows // int类型不是很精确 不过还可以的
			msg := fmt.Sprintf("** Progress Rate:%s%d%%\n", jindutiao.TouchBar(progress_rate, 12), progress_rate)
			Color(100, flag)
			fmt.Println(msg)
		}
		time.Sleep(1 * time.Second)
	}
}


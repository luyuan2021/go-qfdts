package mpkg

import (
	"Myqfdtsproject/jindutiao"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	_dump      = "Qdump"
	_load      = "Qload"
	_mysqlpump = "mysqlpump"
)

var SrcDBAllROWS int
var dballrows int

func checkCmd(args1, args2 string) (flag bool) {
	cmd := exec.Command(args1, args2)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout // 标准输出
	cmd.Stderr = &stderr // 标准错误
	err := cmd.Run()
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())

	if err != nil {
		// 打印输出内容 退出应用程序 defer函数不会执行
		// os.Exit()相比多了打印输出内容
		fmt.Printf("out:\n%s\nerr:\n%s\n", outStr, errStr)
		return false
	}
	return true
}

func CheckMytools() {

	// 检查是否安装mydmper , myloader
	// 可以用lookPath
	//path, err := exec.LookPath("ffmpeg")
	//if err != nil {
	//	log.Fatal("installing fortune is in your future")
	//}
	//fmt.Printf("fortune is available at %s\n", path)
	PrintLog("########依赖环境检查########")
	//var tools = []string{"mydumper", "myloader", "mysqlpump"}
	var tools = []string{_dump, _load, _mysqlpump}

	for _, v := range tools {
		if checkCmd(v, "-V") == false {
			log.Fatalf("操作系统未安装 %s 命令 ，程序退出，异常为：cmd.Run() failed\n", v)
		} else {
			cmd := exec.Command(v, "-V")
			out, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Printf("%s构建信息 :\n%s\n", v, string(out))
				log.Fatalf("cmd.Run() failed with %s\n", err)
			}
			fmt.Printf("%s", string(out))
		}
	}
}

func MyDumper(ip, port, userName, password string, backupdb []string, ignoredb []string, dumperthread string, dumploglevel int, dumpfile string, tablerowssplit, tablecountsthreads int) (err error) {
	PrintLog("############# 基于mydumper进行数据备份 #############")
	bigVer := strings.Split(MySqlVersion, ".")[0] + "." + strings.Split(MySqlVersion, ".")[1]
	// 如果是8.0以上的版本，需要先执行如下查询
	// show table status会导致其他会话长时间卡住
	if bigVer == "8.0" {
		prepareSql := "select * from information_schema.tables"
		for i := 0; i <= 10; i++ {
			_, _ = DB.Query(prepareSql)
		}
	}

	// 5.6,5.7不需要进行上方操作
	// 组装mydumper正则表达式
	// '^(?!(mysql\.|information_schema\.|performance_schema\.|sys\.))'

	agrs1 := "'^(?!("
	i := 1
	for _, v := range ignoredb {
		if i < len(ignoredb) {
			agrs1 = agrs1 + v + "\\.|"
		} else {
			agrs1 = agrs1 + v + "\\"
		}
		i++
	}
	ignoreRegx := agrs1 + ".))'"
	//fmt.Println(ignoreRegx)

	/* 这段代码cmdArgs一直有问题 不能很好的写出database的参数，而且--database 参数无法一次备份多个数据库*/
	//cmdArgs := []string{"--user", userName, "--password", password, "-h", ip, "-P", port, "--database",dbslice[0],dbslice[1],dbslice[2],dbslice[3],"--triggers", "--events", "--routines", "--threads", dumperthread, "--less-locking", "--build-empty-files", "--verbose", "3", "--outputdir", pwd,"\""}
	//ret, err := runCmd("mydumper", cmdArgs)
	// 打印出mydumper命令的执行结果 完全的屏幕输出
	//fmt.Println(ret)

	//doRegx
	var cmd string
	/*   这些都不要了
	//var dbslice []string
	//if len(backupdb) == 0 && len(ignoredb)==0 {
	//	fmt.Println("[Notice]: 配置文件中未填写要备份的数据库的名称")
	//	fmt.Println("[Notice]: 默认备份非系统数据库，即不备份 [mysql,sys,informance_schema,performance_schema] ")
	//	db:=GetDBNAME()
	//	fmt.Println(db)
	//
	//	for _,v :=range db{
	//		dbslice=append(dbslice,v)
	//	}
	//	cmd="mydumper --user "+userName+" --password "+password+" -h "+ ip + " -P "+ port+" --database " +strings.Join(dbslice," ") + " --triggers --events --routines --threads "+ dumperthread + " --less-locking --build-empty-files --verbose 3 --outputdir " + pwd + " --logfile " + s1 +"/mydump.log"
	//
	//}else if len(backupdb) >=1 {
	//	cmd="mydumper --user "+userName+" --password "+password+" -h "+ ip + " -P "+ port+" --database " +strings.Join(backupdb," ") + " --triggers --events --routines --threads "+ dumperthread + " --less-locking --build-empty-files --verbose 3 --outputdir " + pwd + " --logfile " + s1 +"/mydump.log"
	//}
	*/

	/* 统计以下Src实例全部的表的行数总量 */

	PrintLog("准备开始download数据，由于download数据时间长，建议重新开一个窗口 tail -f mydump.log")
	s1, _ := os.Getwd()
	dump := _dump
	cmd = dump + " --user " + userName + " --password " + password + " -h " + ip + " -P " + port + " --regex " + ignoreRegx + " --triggers --events --routines --threads " + dumperthread + " --less-locking --build-empty-files " + "--rows " + strconv.Itoa(tablerowssplit) + " --verbose " + strconv.Itoa(dumploglevel) + " --outputdir " + dumpfile + " --logfile " + s1 + "/Qdump.log"
	//cmd = dump + " --user " + userName + " --password " + password + " -h " + ip + " -P " + port + " --regex " + ignoreRegx + " --triggers --events --routines --threads " + dumperthread + " --less-locking --build-empty-files --verbose " + strconv.Itoa(dumploglevel) + " --outputdir " + dumpfile

	PrintLog(cmd)

	dballrows = AllRowsCount(ip, port, userName, password, tablecountsthreads)
	var wait sync.WaitGroup
	ctxread, stop := context.WithCancel(context.Background())
	wait.Add(1)
	go func() {
		defer wait.Done()
		// 统计当前的rows_read的值
		initread := mysqlstatus(ip, port, userName, password, "row_read")
		canalctxread(ctxread, ip, port, userName, password, initread, dballrows)
	}()

	out := string(Cmd(cmd, true))
	PrintLog(out)
	if err != nil {
		// 打印输出内容 退出应用程序 defer函数不会执行
		// log.Fatal()与os.Exit()相比多了打印输出内容
		log.Fatalf("dumper数据异常,code : %s", err)
	}

	stop()
	wait.Wait()

	return
}

func canalctxread(ctx context.Context, dbip, dbport, dbuserName, dbpassword string, initread, allrows int) {
	for {
		select {
		case <-ctx.Done():
			Color(100, "Dump data complete...")
			return
		default:
			flag := "Dumping data..."
			nowread := mysqlstatus(dbip, dbport, dbuserName, dbpassword, "row_read")
			progress_rate := (nowread - initread) * 100 / allrows // int类型不是很精确 不过还可以的
			msg := fmt.Sprintf("** Progress Rate:%s%d%%\n", jindutiao.TouchBar(progress_rate, 12), progress_rate)
			Color(100, flag)
			fmt.Println(msg)
		}
		time.Sleep(1 * time.Second)
	}
}

func runCmd(commandName string, params []string) (string, error) {
	cmd := exec.Command(commandName, params...)
	// fmt.Println("Cmd", cmd.Args)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return "", err
	}
	err = cmd.Wait()
	return out.String(), err
}

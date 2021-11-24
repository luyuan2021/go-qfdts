package main

import (
	"Myqfdtsproject/globalconfig"
	"Myqfdtsproject/license"
	"Myqfdtsproject/mpkg"
	"Myqfdtsproject/storage"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql" //如果只希望导入包，而不使用包内部的数据时，可以使用匿名导入包。具体的格式如下：import _ "包的路径"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	//WelcomeTxt="welcome"
	//WelcomeTxt = "====================================欢迎使用 qfusion for mysql 数据迁移服务====================================\n" +
	//	"1、本工具采用纯go语言环境开发，有较高的性能。\n" +
	//	"2、通过本工具可以实现mysql数据的跨云的 *全量* 数据迁移。基于binlog的增量数据同步，也会在后续的版本中加入支持。\n" +
	//	"3、说明: \n" +
	//	"  本工具底层封装了mydumper,在使用时请尽量下载最新版的mydumper。\n" +
	//	"  由于mydumper对于mysql 8.0的支持并不是很完美，在FLUSH TABLES过程中，可能会出现大量元数据锁的场景，本工具已经通过sql解决这一问题。\n" +
	//	"  Mysql 8.0目前已知的两个迁移问题： \n    1、账号采用8.0默认加密插件的，不能直接用select user这种方式迁移,工具中集成了其他工具功能来实现这一需求\n    2、FTWRL时，与`show table status`语句相互影响,产生MDL lock问题\n" +
	//	"  Mydumper现已经发布的最新版mydumper 0.11.2, built against MySQL 8.0.21 myloader 0.11.2, built against MySQL 8.0.21\n" +
	//	"  8.0的备份问题已经解决\n" +
	//	"===============================================================================================================\n"

	_step          = 8 //定义一共有多少个步骤
	_stepusageinfo = "输入以下序号以决定执行工具的哪一步\n" +
		"(1) 收集上游数据库的统计信息及完成数据备份\n" +
		"(2) 进行用户迁移\n" +
		"(3) 检查和修改上游数据库备份文件中的非innodb表和row_format参数等问题\n" + //表字段过长 row size too large等改为Myisam
		"(4) 将备份文件并行写入下游数据库\n" +
		"(5) 收集下游数据库等统计信息\n" +
		"(6) 进行数据校验任务\n" +
		"(7) 进行增量数据同步\n" +
		"(8) 进行增量数据同步状态检查"

	_backupdir = "/databackup"
)

var (
	SrchostIp, SrcUser, SrcUserPasswd, DsthostIp, DstUser, DstUserPasswd, SrchostPort, DsthostPort string
	BackUpDB, IngoreDB, IngoreUser                                                                 []string
	DumpThread, LoadThread                                                                         string
	Dumploglevel, Loadloglevel, Tablecountsthread                                                  int
	//ReplIgnoreDB                                                                                   []string
)

var SrcVer, DstVer string
var MyPumpCUSQL string
var cfg *globalconfig.ConfigInfo
var DumpfileDir string

func DoSrcDB() {
	//收集上游数据库的统计信息
	mpkg.PrintLog("#############收集上游数据库的统计信息#############")
	_ = mpkg.ConnectDB(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd)
	defer mpkg.CloseDB()

	_ = mpkg.CountDBSize()
	_ = mpkg.GetVET()

	mpkg.GetVariable("src")

	storage.UpdatePos(0)

}

func modify() {
	// Modify
	mpkg.ModifyData(DumpfileDir)
}

func createdstuser56_57() {
	_ = mpkg.ConnectDB(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd)
	_ = mpkg.GetUser(IngoreUser)
	mpkg.CloseDB()

	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	mpkg.CreateUser()
	mpkg.CloseDB()

}
func DoDstDB() {
	fmt.Println("#############收集下游数据库的统计信息#############")
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	defer mpkg.CloseDB()
	_ = mpkg.CountDBSize()
	_ = mpkg.GetVET()

}

func setsqlmode() {
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	defer mpkg.CloseDB()
	_ = mpkg.SetVariables("sql_mode", "", "global")
	_ = mpkg.SetVariables("sql_mode", mpkg.VarSqlMode, "global")
}
func GetVersion() {
	_ = mpkg.ConnectDB(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd)
	SrcVer, _ = mpkg.GetdbVersion("src")
	mpkg.CloseDB()

	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	DstVer, _ = mpkg.GetdbVersion("dst")
	mpkg.CloseDB()
	log.Println("########数据库版本信息########")
	mpkg.Color(100, fmt.Sprintf("上游DB: %s", SrcVer))
	mpkg.Color(100, fmt.Sprintf("下游DB: %s\n", DstVer))

}

func mysqlpump() {

	MyPumpCUSQL, _ = mpkg.MysqlPump(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd, IngoreUser)
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	mpkg.CreateUserfor80(MyPumpCUSQL)
	mpkg.CloseDB()
}

func main() {
	/* 验证license */
	license.Checklicense()
	var filepath, stepstep, steponly string

	SrcConn := make(map[string]string)
	DstConn := make(map[string]string)
	Welcome()
	flag.StringVar(&filepath, "f", "dts_config.json", "配置文件名称，建议是全路径")
	flag.StringVar(&stepstep, "step", "0", _stepusageinfo)
	flag.StringVar(&steponly, "steponly", "no", "是否只执行输入的步骤,默认会从输入的步骤一直往后执行")
	flag.Parse()

	s1, _ := os.Getwd()
	DumpfileDir = s1 + _backupdir

	var steponce bool
	if a := strings.ToLower(steponly); a == "yes" {
		steponce = true
	} else {
		steponce = false
	}

	/* new func test */
	cfg = globalconfig.Cfg(filepath)

	/* Src conn info */
	SrcConn["IP"] = cfg.SrchostIp
	SrcConn["PORT"] = cfg.SrchostPort
	SrcConn["USER"] = cfg.SrcUser
	SrcConn["PASSWORD"] = cfg.SrcUserPasswd
	/* Dst conn info */
	DstConn["IP"] = cfg.DsthostIp
	DstConn["PORT"] = cfg.DsthostPort
	DstConn["USER"] = cfg.DstUser
	DstConn["PASSWORD"] = cfg.DstUserPasswd

	/* 读取配置文件获得配置信息 */
	connInfo := mpkg.GetConfig(filepath)

	/* src info */
	SrchostIp = connInfo.SrchostIp
	SrcUser = connInfo.SrcUser
	SrcUserPasswd = connInfo.SrcUserPasswd
	SrchostPort = connInfo.SrchostPort
	/*dst info */
	DsthostIp = connInfo.DsthostIp
	DstUser = connInfo.DstUser
	DstUserPasswd = connInfo.DstUserPasswd
	DsthostPort = connInfo.DsthostPort

	/* 全局配置 */
	BackUpDB = connInfo.DoDB
	IngoreDB = connInfo.IngoreDB
	IngoreUser = connInfo.IngoreUser

	/* Mydumper Myloader配置 */
	/* 备份线程设置 */
	DumpThread = connInfo.MydumperConfig.DumpThread
	LoadThread = connInfo.MyloaderConfig.LoadThread
	/* 日志级别 */
	Dumploglevel = connInfo.MydumperConfig.Dumploglevel
	Loadloglevel = connInfo.MyloaderConfig.Loadloglevel
	/* 复制过滤数据库 */
	//ReplIgnoreDB = connInfo.ReplIgnoreDB

	/* 如果配置文件中不写dumper线程和loader线程的个数 对这两个参数进行初始化 */

	if DumpThread == "" {
		DumpThread = "1"
	}
	if LoadThread == "" {
		LoadThread = "10"
	}

	/* 表行数对比 并行统计的线程个数 */
	Tablecountsthread = connInfo.Tablecountsthread
	/* set default value is 10*/
	if Tablecountsthread == 0 {
		Tablecountsthread = 10
	}

	// 指定备份的数据库名称和忽略备份的数据库名称不能同时为空
	// 指定备份的数据库名称和忽略备份的数据库名称只能写一个
	if len(BackUpDB) == 0 && len(IngoreDB) == 0 {
		mpkg.Color(102, "ERROR: ")
		mpkg.Color(0, "配置文件输入有误，指定备份的数据库名称和忽略备份的数据库名称不能同时为空")
		fmt.Println()
		os.Exit(1)
	} else if len(BackUpDB) > 0 && len(IngoreDB) > 0 {
		mpkg.Color(102, "ERROR: ")
		mpkg.Color(0, "配置文件输入有误，指定备份的数据库名称和忽略备份的数据库名称只能写一个")
		fmt.Println()
		os.Exit(2)
	}

	GetVersion()

	/* 这里实现脚本执行到了哪一步*/
	/* 这里实现step执行步骤 */
	storage.Initialize()
	if stepstep != "" {
		s, _ := strconv.Atoi(stepstep)
		storage.UpdatePos(s)
	}
	//if i, _ := strconv.Atoi(storage.ReadPos()); i == _step {
	//	mpkg.PrintLog(fmt.Sprintf("请先清空%s文件", storage.FlagFilePath))
	//	os.Exit(1)
	//}
	for i, _ := strconv.Atoi(storage.ReadPos()); i <= _step; i++ {
		switch i {
		case 0:
			step0()
			ifstepOnce(steponce)
		case 1:
			step1()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 2:
			step2()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 3:
			step3()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 4:
			step4()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 5:
			step5()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 6:
			step6()
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 7:
			step7(SrcConn, cfg.ReplIgnoreDB)
			//fmt.Printf(strconv.Itoa(i))
			ifstepOnce(steponce)
		case 8:
			step8()
			ifstepOnce(steponce)
		default:
			mpkg.PrintLog("THIS IS THE FIRST PLAY")
			step0()
			ifstepOnce(steponce)
		}

	}
	Goodbye()
}
func step0() {
	/*检查工具是否安装*/
	mpkg.CheckMytools()
}

func step2() {
	GetVersion()
	// create user
	if mpkg.SrcBigVer == "5.6" || mpkg.SrcBigVer == "5.7" {
		createdstuser56_57()
	} else {
		mpkg.PrintLog("USE MYSQLPUMP做数据迁移")
		_ = mpkg.ConnectDB(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd)
		_ = mpkg.GetUser(IngoreUser)
		mpkg.CloseDB()
		mysqlpump()
	}
	storage.UpdatePos(2)
	DoSrcDB()
}

func step1() {
	// 迁移
	_ = mpkg.MyDumper(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd, BackUpDB, IngoreDB, DumpThread, Dumploglevel, DumpfileDir)
	storage.UpdatePos(1)
}

func step3() {

	modify()
	storage.UpdatePos(3)
}

func step4() {
	setsqlmode()
	_ = mpkg.MyLoader(DsthostIp, DsthostPort, DstUser, DstUserPasswd, LoadThread, Loadloglevel, DumpfileDir)
	storage.UpdatePos(4)
}

func step5() {
	DoDstDB()
	storage.UpdatePos(5)
}

func step6() {
	// 进行数据统计
	mpkg.PrintLog("#############进行数据行数校验暂不支持花里花哨的数据库名称，如`testdb 1` ,'testdbv1.0' etc. ")
	t, err := strconv.Atoi(cfg.TableCheckConfig.Tablecountsthread)
	if err != nil {
		mpkg.Color(102, "* Error")
		log.Println(err)
	}
	mpkg.Tablecount(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd, DsthostIp, DsthostPort, DstUser, DstUserPasswd, t)
	storage.UpdatePos(6)
}

func step7(SrcConn map[string]string, replingoredb []string) {
	mpkg.PrintLog("########## 配置下游数据库到上游数据库到MASTER,SLAVE关系 ##########")
	metadatafile := DumpfileDir + "/metadata"
	cmd := "cat " + metadatafile + "| grep -v Started | grep -v Finished | grep -v \"SHOW MASTER STATUS\" | grep -v \"Log:\" | grep -v \"Pos\" | tr \"\\n\" \" \" | awk -F \"GTID:\" '{print $2}' | awk -F \"SHOW SLAVE STATUS\" '{print$1}' | sed s/[[:space:]]//g"
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	defer mpkg.CloseDB()
	mpkg.MS(cmd, SrcConn, replingoredb)
	storage.UpdatePos(7)
}

func step8() {
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	defer mpkg.CloseDB()
	mpkg.GetMSStatus()
	storage.UpdatePos(8)

}

func ifstepOnce(s bool) {
	if s == true {
		os.Exit(0)
	}

}
func Welcome() {
	WelcomeTxt := `______________________________
\                             \           _         ______ |
 \                             \        /   \___-=O'/|O'/__|
  \      QFDTS , Here we go !!  \_______\          / | /    )
  /                             /        '/-==__ _/__|/__=-|  -GM
 /        Qfusion Cloud        /         *             \ | |
/                             /                        (o)
------------------------------
`
	startMsg := "if you have any problem, please send your message to ch1yanzhi@163.com\n\n"
	//LOG.Warn(fmt.Sprintf("\n%s\n%s\n", WelcomeTxt, startMsg))
	fmt.Printf("\n%s\n%s\n", WelcomeTxt, startMsg)
}

func Goodbye() {
	goodbye := `
                ##### | #####
Oh we finish ? # _ _ #|# _ _ #
               #      |      #
         |       ############
                     # #
  |                  # #
                    #   #
         |     |    #   #      |        |
  |  |             #     #               |
         | |   |   # .-. #         |
                   #( O )#    |    |     |
  |  ################. .###############  |
   ##  _ _|____|     ###     |_ __| _  ##
  #  |                                |  #
  #  |    |    |    |   |    |    |   |  #
   ######################################
                   #     #
                    #####
`

	//LOG.Warn(goodbye)
	fmt.Printf(goodbye)
}

func ProDisplayXDR(){
	fmt.Println("这里我在5W的显示器上写的代码，这段代码值5W")
}
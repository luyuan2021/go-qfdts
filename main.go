package main

import (
	"Myqfdtsproject/mpkg"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql" //如果只希望导入包，而不使用包内部的数据时，可以使用匿名导入包。具体的格式如下：import _ "包的路径"
	"os"
)

const (
	Welcome_txt = "====================================欢迎使用 qfusion for mysql 数据迁移服务====================================\n" +
		"1、本工具采用纯go语言环境开发，有较高的性能。\n" +
		"2、通过本工具可以实现mysql数据的跨云的 *全量* 数据迁移。基于binlog的增量数据同步，也会在后续的版本中加入支持。\n" +
		"3、说明: \n" +
		"  本工具底层封装了mydumper,在使用时请尽量下载最新版的mydumper。\n" +
		"  由于mydumper对于mysql 8.0的支持并不是很完美，在FLUSH TABLES过程中，可能会出现大量元数据锁的场景，本工具已经通过sql解决这一问题。\n" +
		"  Mysql 8.0目前已知的两个迁移问题： \n    1、账号采用8.0默认加密插件的，不能直接用select user这种方式迁移\n    2、FTWRL时，产生MDL lock问题\n" +
		"===============================================================================================================\n"
)

var (
	SrchostIp, SrcUser, SrcUserPasswd, DsthostIp, DstUser, DstUserPasswd, SrchostPort, DsthostPort string
	BackUpDB, IngoreDB                                                                             []string
	DumpThread, LoadThread                                                                         string
	Dumploglevel, Loadloglevel                                                                     int
)

func main() {
	var filepath string
	fmt.Println(Welcome_txt)
	flag.StringVar(&filepath, "f", "", "配置文件路径，建议是全路径")
	flag.Parse()
	fmt.Println("config file is: ", filepath)

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

	BackUpDB = connInfo.DoDB
	IngoreDB = connInfo.IngoreDB
	DumpThread = connInfo.MydumperConfig.DumpThread
	LoadThread = connInfo.MyloaderConfig.LoadThread

	Dumploglevel=connInfo.MydumperConfig.Dumploglevel
	Loadloglevel=connInfo.MyloaderConfig.Loadloglevel


	/* 如果配置文件中不写dumper线程和loader线程的个数 对这两个参数进行初始化 */

	if DumpThread == "" {
		DumpThread = "4"
	}
	if LoadThread == "" {
		LoadThread = "10"
	}

	// 指定备份的数据库名称和忽略备份的数据库名称不能同时为空
	// 指定备份的数据库名称和忽略备份的数据库名称只能写一个
	if len(BackUpDB) == 0 && len(IngoreDB) == 0 {
		fmt.Println("配置文件输入有误，指定备份的数据库名称和忽略备份的数据库名称不能同时为空")
		os.Exit(1)
	} else if len(BackUpDB) > 0 && len(IngoreDB) > 0 {
		fmt.Println("配置文件输入有误，指定备份的数据库名称和忽略备份的数据库名称只能写一个")
		os.Exit(2)
	}

	DoSrcDB()
	// 迁移
	_ = mpkg.MyDumper(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd, BackUpDB, IngoreDB, DumpThread,Dumploglevel)
	modify()
	mpkg.MyLoader(DsthostIp,DsthostPort,DstUser,DstUserPasswd,LoadThread,Loadloglevel)

	//DoDstDB()

}

func DoSrcDB() {
	//收集上游数据库的统计信息
	fmt.Println("现在开始收集上游库的配置信息")
	_ = mpkg.ConnectDB(SrchostIp, SrchostPort, SrcUser, SrcUserPasswd)
	_ = mpkg.GetdbVersion()
	_ = mpkg.CountDBSize()
	_ = mpkg.GetVET()
	_ = mpkg.GetUser()
	defer mpkg.CloseDB()

}

func modify(){
	// Modify
	mpkg.ModifyData()
}

func DoDstDB() {
	fmt.Println("现在开始收集下游库的配置信息")
	_ = mpkg.ConnectDB(DsthostIp, DsthostPort, DstUser, DstUserPasswd)
	_ = mpkg.GetdbVersion()
	_ = mpkg.CountDBSize()
	_ = mpkg.GetVET()
	_ = mpkg.GetUser()
	defer mpkg.CloseDB()
}

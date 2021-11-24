package mpkg

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
)

var DB *sql.DB
var MysqlBigVersion, MySqlVersion string
var SrcBigVer, DstBigVer string
var HasnotInnodbtable bool

type UnInnodbTable struct {
	DBNAME, TABLENAME, TABLEENGINE string
}

var UnInnodbTableInfo []string

//func NewUnInnodbTableInfo(dbname , tablename ,tableengine string) *UnInnodbTable {
//	return &UnInnodbTable{
//		DBNAME: dbname,
//		TABLENAME:  tablename,
//		TABLEENGINE:tableengine,
//	}
//}

//收集数据库的统计信息
//将统计信息打印到屏幕并打印到日志中，通过interface去实现这个功能
func GetdbVersion(role string) (mysqlbigversion string, err error) {
	verText := "select version() as MYSQLVERSION"
	ret, err := DB.Query(verText)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	type dbVersion struct {
		mysqlVerk string
	}
	var v dbVersion
	for ret.Next() {
		ret.Scan(&v.mysqlVerk)
	}
	myVer := strings.Split(v.mysqlVerk, ".")
	tmpVer := strings.Split(myVer[2], "-")[0]
	myVer = append(myVer[:2], myVer[3:]...)
	MysqlBigVersion = myVer[0] + "." + myVer[1]
	MySqlVersion = myVer[0] + "." + myVer[1] + "." + tmpVer
	if role == "src" {
		SrcBigVer = MysqlBigVersion
	} else if role == "dst" {
		DstBigVer = MysqlBigVersion
	}

	return MySqlVersion, nil
}

func ConnectDB(ip, port, userName, password string) (err error) {
	mysqlUri := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", "?charset=utf8"}, "")
	DB, err = sql.Open("mysql", mysqlUri)
	if err := DB.Ping(); err != nil {
		text := fmt.Sprintf("连接%v@%v失败", ip, port)
		PrintLog(text)
		os.Exit(1)
		return err
	}
	return
}

func GetDBNAME() (db []string) {
	sqlText := fmt.Sprintf("select SCHEMA_NAME  from information_schema.SCHEMATA where SCHEMA_NAME not in (%s,%s,%s,%s) ", "'mysql'", "'information_schema'", "'performance_schema'", "'sys'")
	//fmt.Println(sql)
	ret, err := DB.Query(sqlText)
	if err != nil {
		fmt.Println(err)
	}
	var dbname []string
	DBNAME := ""
	for ret.Next() {
		_ = ret.Scan(&DBNAME)
		dbname = append(dbname, DBNAME)
	}

	return dbname
}

func CountDBSize() (err error) {
	dbname := GetDBNAME()

	for _, v := range dbname {
		// Statistics database size 统计数据库表的大小 转换成G显示
		sqlText := "SELECT  TABLE_SCHEMA,table_name,ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024/1024,4)as G,ENGINE" +
			" FROM " +
			" information_schema.tables" +
			" WHERE table_schema= '" +
			v +
			"' AND ENGINE is not NULL " +
			"  ORDER BY G DESC;"

		rows, err := DB.Query(sqlText)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type tableSize struct {
			tableSchema, tableName, tableEngine string
			tableSize                           string //这里如果是null值会scan报错
		}

		for rows.Next() {
			i := 0
			var t tableSize
			err := rows.Scan(&t.tableSchema, &t.tableName, &t.tableSize, &t.tableEngine)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			fmt.Printf("database_name:%v,table_name:%v,table_size(G):%v,table_engin:%v\n", t.tableSchema, t.tableName, t.tableSize, t.tableEngine)
			if !(t.tableEngine == "InnoDB") {
				i++
				// 是否在这里加一个 slice,在往对端数据库回灌数据的时候，先判断下有没有非Innodb表，并将数据库和表名称记录下来。后面对sql文件做处理。
			}

			if i > 0 {
				rmt := fmt.Sprintf("[Warning]: The Engine of table %v.%v is %v,not InnoDB\n", t.tableSchema, t.tableName, t.tableEngine)
				Color(101, rmt)
				HasnotInnodbtable = true
				//t1:=NewUnInnodbTableInfo(t.tableSchema,t.tableName,t.tableEngine)
				info := t.tableSchema + "===" + t.tableName + "===" + t.tableEngine
				UnInnodbTableInfo = append(UnInnodbTableInfo, info)
			}
		}

	}

	return
}
func GetVET() (err error) {
	fmt.Println("================================ VIEW INFO ================================")
	dbname := GetDBNAME()
	// VIEW
	i := 0
	for _, v := range dbname {
		viewSql := "SELECT TABLE_SCHEMA, TABLE_NAME, DEFINER FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '" +
			v +
			"' ORDER BY TABLE_NAME ASC"
		rows, err := DB.Query(viewSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type viewInfo struct {
			viewSchema string
			viewName   string
			definEr    string
		}

		for rows.Next() {
			var t viewInfo
			err := rows.Scan(&t.viewSchema, &t.viewName, &t.definEr)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			fmt.Printf("DBNAME: %v, TABLENAME: %v,  DEFINER: %v \n", t.viewSchema, t.viewName, t.definEr)
			i++
		}
	}
	if i == 0 {
		fmt.Println("所有的DB不存在view")
	}

	// PROCEDURE
	fmt.Println("================================ PROCEDURE INFO ================================")
	j := 0
	for _, v := range dbname {
		procSql := "SHOW PROCEDURE STATUS WHERE Db ='" + v + "'"
		rows, err := DB.Query(procSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type proceDure struct {
			dbName                                                                                               string
			procName                                                                                             string
			procType                                                                                             string
			deFiner                                                                                              string
			modiFied, creaTed, securityType, comMent, characterSetclient, collationConnection, databaseCollation string
		}
		for rows.Next() {
			var p proceDure
			err := rows.Scan(&p.dbName, &p.procName, &p.procType, &p.deFiner, &p.modiFied, &p.creaTed, &p.securityType, &p.comMent, &p.characterSetclient, &p.collationConnection, &p.databaseCollation)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			fmt.Printf("DBNAME: %v,  PROCNAME: %v,  DEFINER: %v\n", p.dbName, p.procName, p.deFiner)
			j++
		}
	}
	if j == 0 {
		PrintLog("所有的DB不存在PROC")
	}
	// FUNCTION
	fmt.Println("================================ FUNCTION INFO ================================")
	k := 0
	for _, v := range dbname {
		procSql := "SHOW FUNCTION STATUS WHERE Db ='" + v + "'"
		rows, err := DB.Query(procSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type proceDure struct {
			dbName                                                                                               string
			funcName                                                                                             string
			procType                                                                                             string
			deFiner                                                                                              string
			modiFied, creaTed, securityType, comMent, characterSetclient, collationConnection, databaseCollation string
		}
		for rows.Next() {
			var p proceDure
			err := rows.Scan(&p.dbName, &p.funcName, &p.procType, &p.deFiner, &p.modiFied, &p.creaTed, &p.securityType, &p.comMent, &p.characterSetclient, &p.collationConnection, &p.databaseCollation)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			fmt.Printf("DBNAME: %v,  FUNCNAME: %v,  DEFINER: %v\n", p.dbName, p.funcName, p.deFiner)
			k++
		}
	}
	if k == 0 {
		PrintLog("所有的DB不存在FUNC")
	}

	// EVENT
	fmt.Println("================================ EVENT INFO ================================")
	l := 0
	for _, v := range dbname {
		eventSql := "SELECT EVENT_SCHEMA, EVENT_NAME, DEFINER FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '" + v + "' ORDER BY EVENT_NAME ASC"

		rows, err := DB.Query(eventSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type eventInfo struct {
			eventSchema, eventName, deFiner string
		}
		for rows.Next() {
			var q eventInfo
			err := rows.Scan(&q.eventSchema, &q.eventName, &q.deFiner)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			fmt.Printf("DBNAME: %v,  EVENTNAME: %v,  DEFINER: %v\n", q.eventSchema, q.eventName, q.deFiner)
			l++
		}
	}
	if l == 0 {
		PrintLog("所有的DB不存在EVENT")
	}

	// TRIGGER
	PrintLog("================================ TRIGGER INFO ================================")
	m := 0
	for _, v := range dbname {
		triggerSql := "select TRIGGER_SCHEMA, TRIGGER_NAME, DEFINER from information_schema.TRIGGERS where TRIGGER_SCHEMA='" +
			v +
			"' order by TRIGGER_SCHEMA ASC"
		rows, err := DB.Query(triggerSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type triGger struct {
			triggerSchema, triggerName, deFiner string
		}
		for rows.Next() {
			var q triGger
			err := rows.Scan(&q.triggerSchema, &q.triggerName, &q.deFiner)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			text := fmt.Sprintf("DBNAME: %v,  TRIGGER NAME: %v,  DEFINER: %v\n", q.triggerSchema, q.triggerName, q.deFiner)
			PrintLog(text)
			m++
		}
	}
	if m == 0 {
		PrintLog("所有的DB不存在trigger")
	}
	return
}

//func QUERYDB(querysql string) (*sql.Rows) {  // 不能直接返回 ret，返回值列表
// 实现一个访问数据库的统一方法，
//var err error
//// DSN:Data Source Name
//dsn := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", "?charset=utf8"}, "")
////fmt.Println(dsn)
//// 不会校验账号密码是否正确
//// 注意！！！这里不要使用:=，我们是给全局变量赋值，然后在main函数中使用全局变量db
//DB, err = sql.Open("mysql", dsn)
//if err != nil {
//	fmt.Println("连接失败")
//	return nil, err
//}
//验证连接
//if err := db.Ping(); err != nil {
//	fmt.Println("连接失败")
//	return nil, err
//} else {
//	fmt.Printf("[Info]: 连接到数据库%v@%v成功\n", userName, ip)
//}
//
//	return DB.Query(querysql)
//
//}

func CloseDB() {
	defer DB.Close()
}

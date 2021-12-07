package mpkg

import (
	"fmt"
	"os"
	"runtime"
	"sync"
)

/*
设定统计并行线程数
var threadNum int  -- 现在已经在配置文件里实现了
*/

/* 要不要做checksum table ？ 可以指定场景，因为执行checksum命令期间 如果接受锁表(LOCK TABLE tablename READ)  */
/* 当然 不停应用做数据校验也没什么意义 */
/* 如下是官方文档对于checksum命令的解释说明*/
/* CHECKSUM TABLE reports a checksum for the contents of a table. You can
use this statement to verify that the contents are the same before and
after a backup, rollback, or other operation that is intended to put
the data back to a known state.

This statement requires the SELECT privilege for the table.

This statement is not supported for views. If you run CHECKSUM TABLE
against a view, the Checksum value is always NULL, and a warning is
returned.

For a nonexistent table, CHECKSUM TABLE returns NULL and generates a
warning.

During the checksum operation, the table is locked with a read lock for
InnoDB and MyISAM.
URL: https://dev.mysql.com/doc/refman/5.7/en/lock-tables.html*/

//  全局定义这个slice
var DbTable []string
var countWait sync.WaitGroup

func Tablecount(srchostIp, srchostPort, srcUser, srcUserPasswd, dsthostIp, dsthostPort, dstUser, dstUserPasswd string, Tablecountsthread int) {
	_ = ConnectDB(srchostIp, srchostPort, srcUser, srcUserPasswd)
	GetDB()
	srctablecout := Process(Tablecountsthread)
	CloseDB()
	_ = ConnectDB(dsthostIp, dsthostPort, dstUser, dstUserPasswd)
	GetDB()
	dsttablecout := Process(Tablecountsthread)
	CloseDB()
	flag := 0
	for k, _ := range srctablecout {
		if srctablecout[k] != dsttablecout[k] {
			text := fmt.Sprintf("[Error]: table %s not equal , src is %v, dst is %v ", k, srctablecout[k], dsttablecout[k])
			PrintLog(text)
			flag++
		} else {
			text := fmt.Sprintf("[Messege]: %s 数据校验通过,行数统计值: %v", k, srctablecout[k])
			PrintLog(text)
		}
	}
	if flag != 0 {
		text := fmt.Sprintf("数据校验不通过,有%v张表数据不一致", flag)
		PrintLog(text)
		os.Exit(1)
	}
}

func GetDB() {
	var sql_get_db string
	var dbname []string

	sql_get_db = "select SCHEMA_NAME  from information_schema.SCHEMATA where SCHEMA_NAME not in (?,?,?,?) "
	db, err := DB.Query(sql_get_db, "mysql", "information_schema", "performance_schema", "sys")

	if err != nil {
		PrintLog("query of select DB name for db incur error")
	}
	DBNAME := ""
	for db.Next() {
		db.Scan(&DBNAME)
		dbname = append(dbname, DBNAME)
	}
	//fmt.Printf("数据库中的DB包含\n%v\n",dbname)

	for _, dbname := range dbname {
		GetTableName(dbname)
	}

}

func GetTableName(db_name string) {

	sql1 := "select TABLE_NAME  from information_schema.TABLES  where  TABLE_TYPE ='BASE TABLE' and  TABLE_SCHEMA =? order by TABLE_NAME"
	rows, err := DB.Query(sql1, db_name)

	if err != nil {
		PrintLog("query of select table name for db incur error")
	}
	TableName := ""
	for rows.Next() {
		rows.Scan(&TableName)

		dbTable := db_name + "." + TableName
		DbTable = append(DbTable, dbTable)
	}
}

func CountNum(db_tablename []string) (a map[string]int) {
	var sql2 string
	var count int
	result := make(map[string]int)
	for _, v := range db_tablename {
		sql2 = "select count(*) from " + v + ";"

		num, err := DB.Query(sql2)
		if err != nil {
			PrintLog("query of select table name for db incur error")
		}
		for num.Next() {
			num.Scan(&count)
		}

		result[v] = count
	}
	return result
}

func Process(n int) (tablecount map[string]int) {

	defer DB.Close()

	text := fmt.Sprintf("[Info]: 进行表统计任务拆分，表总量为%v个,拆分为%v个任务,每个job统计%v个表的行数", len(DbTable), n, len(DbTable)/n)
	PrintLog(text)
	j := 0
	k := 0
	ch1 := make(chan map[string]int, n)
	tableCount := make(map[string]int)
	for i := 1; i <= n; i++ {
		k = j
		if i <= n-1 {
			j = j + len(DbTable)/n
		} else {
			j = len(DbTable)
		}

		slice := DbTable[k:j]

		countWait.Add(1)
		runtime.GOMAXPROCS(2)

		go func(slice []string) {
			ch1 <- CountNum(slice)

			countWait.Done()

		}(slice)

	}
	countWait.Wait()

	close(ch1)
	for tmp := range ch1 {
		for k, v := range tmp {
			tableCount[k] = v

		}
	}

	return tableCount
}

func TableCheckSum() {
	/*这块儿如果是数据校验 最好是做成 table struct table checksum的数据校验 但是这部分的内容实现起来比较麻烦
	临时先参考tidb的sync_diff_inspector 可以直接把这个工具的代码集成到这里
	看下这个工具的diff.go，很多东西都模版化了
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"

	SELECT
	  	k.column_name,
	  	//t.table_name,
	  	//table_schema
	  FROM
	  	information_schema.table_constraints t
	  	JOIN information_schema.key_column_usage k USING ( constraint_name, table_schema, table_name )
	  WHERE
	  	t.constraint_type = 'PRIMARY KEY'
	  	AND t.table_schema = "ch"
	  	AND t.table_name = "student";
	pt-checksum 利用主从 set RR set row fomart=statement (rc yu statement 不兼容)
	主库执行的checksum写到binlog 从库执行一次 ，对比这两个数据
	可以把思想学过来 写到工具里
	*/



}

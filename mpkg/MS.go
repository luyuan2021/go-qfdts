package mpkg

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

func MS(cmd string, srcconn map[string]string, replingoredb []string) {
	gtid := string(Cmd(cmd, true))
	gtid = strings.Replace(gtid, "\n", "", -1)
	setgtid := "set global gtid_purged='" + gtid + "'"

	/* 复制过滤 */
	//"CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = ('mysql.%','sys.%','information_schema.%','performance_schema.%');"
	text := func(r []string) (s string) {
		var t string
		i := 1
		for _, v := range r {
			if i < len(r) {
				t = t + "'" + v + ".%',"
			} else {
				t = t + "'" + v + ".%'"
			}
			i++
		}
		return t
	}(replingoredb)

	ignoretable := "CHANGE REPLICATION FILTER REPLICATE_WILD_IGNORE_TABLE = (" +
		text +
		")"

	mssql := "CHANGE MASTER TO " +
		"MASTER_HOST=\"" + srcconn["IP"] + "\", " +
		"MASTER_USER=\"" + srcconn["USER"] + "\", " +
		"MASTER_PASSWORD=\"" + srcconn["PASSWORD"] + "\", " +
		"MASTER_PORT=" + srcconn["PORT"] + ", " +
		"MASTER_AUTO_POSITION = 1, " +
		"MASTER_CONNECT_RETRY = 10"

	sqlcmd := make([]string, 0)
	sqlcmd = append(sqlcmd, "stop slave")
	sqlcmd = append(sqlcmd, "reset slave all")
	sqlcmd = append(sqlcmd, "reset master")
	sqlcmd = append(sqlcmd, setgtid)
	sqlcmd = append(sqlcmd, mssql)
	/* 这里记得加上复制过滤 */
	sqlcmd = append(sqlcmd, ignoretable)
	/*  */
	sqlcmd = append(sqlcmd, "start slave")
	for _, v := range sqlcmd {
		println(v + ";")
		_, _ = DB.Exec(v)
	}
	GetMSStatus()
}
func GetMSStatus() {
	slavestatus := "SHOW SLAVE STATUS"
	queryResult := tryQueryIfAvailable(DB, slavestatus)
	//for k,v :=range queryResult[0]{
	//	fmt.Println(k,"  :  ",v)
	//}
	if 0 == len(queryResult) {
		log.Println("show slave empty, assume it is a master")
	} else {
		//log.Println("it is a slave")
		queryResult[0] = changeKeyCase(queryResult[0])

		if 1 == len(queryResult) {
			// Multi source replication
			log.Println("show slave multi rows, assume it is a multi source replication")

			var maxLag int64 = -1
			//var totalRelayLogSpace int64 = 0
			for i, resultMap := range queryResult {
				resultMap = changeKeyCase(resultMap)
				//if resultMap["slave_io_running"] != "Yes" {
				//	log.Printf("%dth row slave_io_running != Yes", i)
				//	queryResult[0]["slave_io_running"] = "No"
				//}
				//if resultMap["slave_sql_running"] != "Yes" {
				//	log.Printf("%dth row slave_sql_running != Yes", i)
				//	queryResult[0]["slave_sql_running"] = "No"
				//}
				//
				if resultMap["slave_io_running"] == "Yes" && resultMap["slave_sql_running"] == "Yes" {
					log.Println("增量同步运行状态is OK")
				} else {
					Color(102, "Error: ")
					PrintLog(fmt.Sprintf("增量同步运行状态is not OK, slave_io_running=%s,slave_sql_running=%s", resultMap["slave_io_running"], resultMap["slave_sql_running"]))
				}
				// get max slave_lag
				if resultMap["seconds_behind_master"] != "NULL" && convStrToInt64(resultMap["seconds_behind_master"]) > maxLag {
					log.Printf("%d th row seconds_behind_master may be max", i)
					maxLag = convStrToInt64(resultMap["seconds_behind_master"])
					queryResult[0]["seconds_behind_master"] = resultMap["seconds_behind_master"]
				}

				////get total relay_log_space
				//log.Printf("%dth row relay_log_space is %s", i, resultMap["relay_log_space"])
				//totalRelayLogSpace += convStrToInt64(resultMap["relay_log_space"])
			}
			//queryResult[0]["relay_log_space"] = strconv.FormatInt(totalRelayLogSpace, 10)
		}
	}
}
func changeKeyCase(m map[string]string) map[string]string {
	// change A --> a, B --> b
	lowerMap := make(map[string]string)
	for k, v := range m {
		lowerMap[strings.ToLower(k)] = v
	}
	return lowerMap
}

func tryQueryIfAvailable(db *sql.DB, querys ...string) []map[string]string {
	result := make([]map[string]string, 0, 500)
	for _, q := range querys {
		result = query(db, q)
		if 0 == len(result) {
			log.Println("tryQueryIfAvailable:Got nothing from sql: ", q)
			continue
		}
		return result
	}
	return result
}

func query(db *sql.DB, query string) []map[string]string {

	result := make([]map[string]string, 0, 500)

	rows, err := db.Query(query)
	if nil != err {
		log.Println("db.Query err:", err)
		return result
	}
	defer func(rows *sql.Rows) {
		if rows != nil {
			rows.Close()
		}
	}(rows)

	columnsName, err := rows.Columns()
	if nil != err {
		log.Println("rows.Columns err:", err)
		return result
	}

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columnsName))
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if nil != err {
			log.Println("rows.Next err:", err)
		}

		// Now do something with the data.
		row_map := make(map[string]string)
		for i, col := range values {
			if col == nil {
				row_map[columnsName[i]] = "NULL"
			} else {
				row_map[columnsName[i]] = string(col)
			}
		}
		result = append(result, row_map)
	}

	err = rows.Err()
	if nil != err {
		log.Println("rows.Err:", err)
	}
	return result
}

func convStrToInt64(s string) int64 {
	//log.Println("convStrToInt64: string:", s)
	value := regexp.MustCompile("\\d+").FindString(s)
	i, err := strconv.ParseInt(value, 10, 64)
	if nil != err {
		log.Fatalf("convStrToInt64 err: parse(%v) to int64 err:%v\n", value, err)
	}
	return i
}

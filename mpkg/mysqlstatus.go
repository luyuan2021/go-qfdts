package mpkg

import "log"

func mysqlstatus(dbip, dbport, dbuserName, dbpassword string, flag string) (ret int) {
	_ = ConnectDB(dbip, dbport, dbuserName, dbpassword)
	defer CloseDB()
	sql := ""
	if flag == "row_insert" {
		sql = "show status like 'Innodb_rows_inserted'"
	} else if flag == "row_read" {
		sql = "show status like 'Innodb_rows_read'"
	}

	type sts struct {
		varname  string
		stsvalue int
	}
	rows, err := DB.Query(sql)
	if err != nil {
		log.Println(err)
	}
	var v sts
	for rows.Next() {
		err := rows.Scan(&v.varname, &v.stsvalue)
		if err != nil {
			log.Printf("scan failed, err:%v\n", err)
		}
	}
	return v.stsvalue
}

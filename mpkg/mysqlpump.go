package mpkg

import (
	"fmt"
	"log"
)

func MysqlPump(host, port, user, passwd string, ingoreuser []string) (sql string, err error) {
	/*拼成这么个形式的shell命令，然后调用mysqlpump来做用户账号和权限的导出*/
	/*mysqlpump -uroot -pletsg0 -h127.0.0.1 -P3306 --set-gtid-purged=OFF --exclude-databases=% --users|grep -Ev "root|repl|mysql." >a.sql*/
	log.Printf("上游数据库版本为%s,采用mysqlpump来导出用户", SrcBigVer)
	agrs1 := "\""
    i:=0
	for _, v := range ingoreuser {
		if i < len(ingoreuser)-1 {
			agrs1 = agrs1 + v + "|"
		} else {
			agrs1 = agrs1 + v
		}
		i++
	}
	ignoreRegx:=agrs1+"\""
	fmt.Println(ignoreRegx)
	//cmd := "mysqlpump -h" + host + " -P" + port + " -u" + user + " -p" + passwd + " --set-gtid-purged=OFF --exclude-databases=% --users|grep -Ev \"root|repl|qfsys|heartbeat|mysql.infoschema|mysql.session|mysql.sys\"|grep -E \"CREATE |GRANT\" "
	cmd := "mysqlpump -h" + host + " -P" + port + " -u" + user + " -p" + passwd + " --set-gtid-purged=OFF --exclude-databases=% --users|grep -Ev "+ignoreRegx +"|grep -E \"CREATE |GRANT\" "

	fmt.Println(cmd)
	out := string(Cmd(cmd, true))

	if err != nil {
		// 打印输出内容 退出应用程序 defer函数不会执行
		// log.Fatal()与os.Exit()相比多了打印输出内容
		log.Fatalf("dumper数据异常,code : %s", err)

	}
	return out, nil
}

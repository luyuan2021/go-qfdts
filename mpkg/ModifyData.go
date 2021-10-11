package mpkg

import (
	"fmt"
	"strconv"
	"strings"
)

//Modify data adaptively
func ModifyData() {
	text := "\n" +
		"================================ Modify data adaptively ================================\n" +
		"  1、自动将MyISAM、MEMORY引擎表转换为InnoDB表\n" +
		"  2、对于Mysql 5.6 版本 `ROW_FORMAT=FIXED` 表属性已经在5.7以后的版本废弃，自动修改为5.7以后的默认值\n"
	fmt.Println(text)
	modifyRowFixed(DumpfileDir)
	modifyInnodbtable(DumpfileDir)

}
func modifyRowFixed(dir string) {
	cmd1 := "ls " + dir + "  | grep 'schema.sql'|wc -l"
	out1, _ := strconv.Atoi(string(Cmd(cmd1, true)))

	if out1 == 0 {
		fmt.Println("当前备份的数据库不存在表")
	} else {
		cmd2 := "ls " + dir + "  | grep 'schema.sql'"
		out2 := string(Cmd(cmd2, true))
		tableschemafile := strings.Split(out2, "\n")
		fmt.Println(tableschemafile)
		for _, v := range tableschemafile {
			if v != "" {
				file := dir + "/" + v
				flag := "s/ROW_FORMAT=FIXED//g"
				cmd := "sed -i -e " + "\"" + flag + "\"" + " " + file
				//fmt.Println(cmd)
				Cmd(cmd, true)
			}

		}
	}

}

func modifyInnodbtable(dir string) {
	info := "=============================== 调整非innodb表为innodb表 ==============================="
	fmt.Println(info)
	if len(UnInnodbTableInfo) == 0 {
		fmt.Println("备份文件中没有非InnoDB表")
	} else
	{
		info = info + "\n" + "  1、需要调整的表对应的文件为: "
		fmt.Println(info)
		for _, v := range UnInnodbTableInfo {
			tbSchema := strings.Split(v, "===")[0]
			tbNAME := strings.Split(v, "===")[1]
			tbEngine := strings.Split(v, "===")[2]
			fileName := tbSchema + "." + tbNAME + "-schema.sql"
			fmt.Println("    ", fileName)
			flag := "s/" + tbEngine + "/InnoDB/g"

			cmd := "sed -i -e " + "\"" + flag + "\"" + " " + dir + "/" + fileName
			//fmt.Println(cmd)
			Cmd(cmd, true)

		}
	}

}

/* 如下是对备份文件对说明 */
/* 1、按照schema来看，每个DB都有一个 DBNAME-schema-create.sql 文件 即create database ... */
/* 2、每个表各有两个文件，即表定义文件: DBNAME.TABLENAME-schema.sql和 数据文件: DBNAME.TABLENAME.sql */
/* 对表存储引擎和ROW_FORMAT的修改即修改表定义文件*/
//test1.chtest-schema.sql
//test1.chtest-schema-triggers.sql
//test1.myview-schema.sql
//test1.myview-schema-view.sql
//test1-schema-create.sql
//test1-schema-post.sql

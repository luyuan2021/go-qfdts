package mpkg

import (
	"fmt"
	"log"
	"strings"
)

var SqlSlice = make([]string, 0, 10)
var UserSlice = make([]string, 0, 10)
var GrantSlice = make([]string, 0, 10)

func GetUser(ingoreuser []string) (err error) {
	var userSql string

	agrs1 := "("
	i := 0
	for _, v := range ingoreuser {
		if i < len(ingoreuser)-1 {
			agrs1 = agrs1 + "'" + v + "',"
		} else {
			agrs1 = agrs1 + "'" + v + "'"
		}
		i++
	}
	ignoreRegx := agrs1 + ")"
	fmt.Println(ignoreRegx)

	/*判断一次大版本号*/
	if SrcBigVer == "5.6" {
		userSql = "select" + " user ,host,plugin,Password from mysql.user where user not in " + ignoreRegx
		//userSql = "select" + " user ,host,plugin,Password from mysql.user where user not in ('root','mysql.session','mysql.sys','mysql.infoschema','repl','qfsys','heartbeat')"
	} else if SrcBigVer == "5.7" {
		/*for 5.7*/
		userSql = "select" + " user ,host,plugin,authentication_string from mysql.user where user not in " + ignoreRegx
		//userSql = "select" + " user ,host,plugin,authentication_string from mysql.user where user not in ('root','mysql.session','mysql.sys','mysql.infoschema','repl','qfsys','heartbeat')"
	} else if SrcBigVer == "8.0" {
		userSql = "select" + " user ,host,plugin,authentication_string from mysql.user where user not in " + ignoreRegx
		//userSql = "select" + " user ,host,plugin,authentication_string from mysql.user where user not in ('root','mysql.session','mysql.sys','mysql.infoschema','repl','qfsys','heartbeat')"
	}

	rows, err := DB.Query(userSql)
	if err != nil {
		fmt.Println(err)
		return err
	}
	type userInfo struct {
		userName, userHost, plugin, authenticationString string
	}

	for rows.Next() {
		var u userInfo
		err := rows.Scan(&u.userName, &u.userHost, &u.plugin, &u.authenticationString)
		if err != nil {
			fmt.Printf("scan failed, err:%v\n", err)
			return err
		}
		createuserSql := "CREATE USER '" + u.userName + "'@'" + u.userHost + "' IDENTIFIED WITH " + u.plugin + " BY '" + u.authenticationString + "'"

		user := "'" + u.userName + "'@" + "'" + u.userHost + "'"
		SqlSlice = append(SqlSlice, createuserSql)
		UserSlice = append(UserSlice, user)
	}
	//fmt.Println(strings.Join(SqlSlice, ";\n"))

	//这里就已经得到了全部的创建用户的语句了
	/*like this*/
	//create user 'ch'@'%' identified by password '*276D580ECB401C4673747A0DC11CDDC6035A2014'
	//create user 'chenhao'@'%' identified by password '*276D580ECB401C4673747A0DC11CDDC6035A2014'
	//create user 'linjinsen'@'%' identified by password '*79DFB2B7280E9CD5893A30F77E223F9DBAD48877'
	//create user 'testuser'@'%' identified by password '*276D580ECB401C4673747A0DC11CDDC6035A2014'
	//create user 'wangshucai'@'%' identified by password '*47AB657263AF9DF29B72C6CC2DD7EEA818C73504'
	//然后在下游数据库创建用户的时候，去遍历一遍

	//for _, v := range SqlSlice {
	//	fmt.Println(v)
	//}

	// 权限在这里实现。

	for _, v := range UserSlice {
		//fmt.Println(v)
		grantsSql := "show grants for " + v
		//fmt.Println(grantsSql)
		rows, err := DB.Query(grantsSql)
		if err != nil {
			fmt.Println(err)
			return err
		}
		type userGrant struct {
			grants string
		}
		for rows.Next() {
			var g userGrant
			err := rows.Scan(&g.grants)
			if err != nil {
				fmt.Printf("scan failed, err:%v\n", err)
				return err
			}
			GrantSlice = append(GrantSlice, g.grants)
		}
	}
	//here 输出权限的slice
	//fmt.Println(strings.Join(GrantSlice, ";\n"))
	return
}

func CreateUser() {
	fmt.Println("#############创建用户")

	for _, v := range UserSlice {
		sql := "DROP USER IF EXISTS " + v + ";"

		_, err := DB.Exec(sql)
		if err != nil {
			log.Printf("在下游数据库执行%s语句失败", sql)
			log.Println(err)

		}
	}
	fd0 := 0
	for _, v := range SqlSlice {
		sql := v + ";"
		usernametmp := strings.Split(v, "IDENTIFIED")[0]
		username := strings.Split(usernametmp, "USER")[1]
		_, err := DB.Exec(sql)
		if err != nil {
			Color(102, "** Error\r")
			log.Printf("下游数据库创建用户: %s失败", username)
			log.Println(sql)
			log.Println(err)
			fd0++
		}
	}
	if fd0 == 0 {
		Color(100, "** Message\r")
		log.Printf("下游数据库创建用户全部成功")
	}

	fmt.Println("#############创建用户")
	fd1 := 0
	for _, v := range GrantSlice {
		sql := v + ";"

		_, err := DB.Exec(sql)
		if err != nil {
			Color(102, "** Error\r")
			log.Printf("下游数据库，用户授权失败: %s", sql)
			log.Println(err)
			fd1++
		}
	}
	if fd1 == 0 {
		Color(100, "** Message\r")
		log.Printf("下游数据库用户授权全部成功")
	}

}

func CreateUserfor80(sql string) {
	for _, v := range UserSlice {
		sql := "DROP USER IF EXISTS " + v + ";"

		_, err := DB.Exec(sql)
		if err != nil {
			log.Printf("在下游数据库执行%s语句失败", sql)
			log.Println(err)

		}
	}
	fd := 0
	text := strings.Split(sql, "\n")
	for _, v := range text {
		if len(v) != 0 {
			_, err := DB.Exec(v)
			if err != nil {
				Color(102, "** Error")
				log.Printf("在下游数据库执行%s语句失败", v)
				log.Println(err)
				fd++
			}
		}
	}
	if fd == 0 {
		Color(100, "** Message")
		log.Println("创建用户，用户授权成功")
	}
}

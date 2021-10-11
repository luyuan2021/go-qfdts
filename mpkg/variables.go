package mpkg

import (
	"fmt"
	"github.com/fatih/color"
	"log"
	"strings"
	"time"
)

/* 用于实现对参数的监控 */

// 大小写参数 针对于5.7和8.0的版本区分处理

/* 这里想想怎么用map将参数名和参数值对应起来，用key value来存 */

var VarSqlMode string

func Color(kind int, words string) {
	/* 如下是打印颜色*/
	// 我想创建一个告警 WARNING: iiiiiii 关键字是红色 信息是绿色 即这样
	// Create a custom print function for convenience

	//red := color.New(color.FgRed).PrintfFunc()
	//red("Warning")

	// Mix up multiple attributes
	//notice := color.New(color.Bold, color.FgGreen).PrintlnFunc()
	//notice("Don't forget this...")
	switch kind {
	case 100:
		//fmt.Println("绿色为pass 即info")
		//info := color.New(color.Bold, color.FgGreen).PrintlnFunc()
		info := color.New(color.FgGreen).PrintlnFunc()
		info(words)

	case 101:
		//fmt.Println("黄色为告警")
		warning := color.New(color.FgYellow).PrintfFunc()
		warning(words)

	case 102:
		//fmt.Println("红色为错误")
		err := color.New(color.FgRed).PrintfFunc()
		err(words)

	default:
		message := color.New(color.FgWhite).PrintfFunc()
		message(words)
	}


}

func GetVariable(role string) {
	variablesmap := make(map[string]string)
	// sql mode 主从库保持一致
	//lower_case_table_names
	//参数slice 若想加更多的参数 直接在这里面加即可
	var varslice []string
	varslice = append(varslice, "sql_mode", "log_slave_updates")
	for _, value := range varslice {
		variablesmap[value] = GetVariablevalue(value)
	}

	VarSqlMode = variablesmap["sql_mode"]

	/*根据是上游数据库还是下游数据库进行文本输出和逻辑判断*/
	if role == "src" {
		fmt.Println("================================ 上游数据库参数信息 ================================")

		for k, v := range variablesmap {
			fmt.Println(k, ": ", v)
		}

		fmt.Println("=================================================================================")
		if strings.EqualFold(variablesmap["log_slave_updates"], "OFF") || variablesmap["log_slave_updates"] == "0" {

			Color(101, "WARNNING: ")
			Color(100, "上游数据库未开启log_slave_update参数，如果需要做增量同步，则无法进行。")
			fmt.Println()
			// 休眠2s 提示
			time.Sleep(time.Duration(2) * time.Second)
		}

	} else if role == "dst" {
		fmt.Println("================================ 下游数据库参数信息 ================================")
		for k, v := range variablesmap {
			fmt.Println(k, ": ", v)
		}
		fmt.Println("=================================================================================")
	}

}

/*下方函数实现show variables like ""(不支持%匹配)方法，传入要查询的参数，返回参数的值 */
func GetVariablevalue(variables string) (value string) {
	sql := fmt.Sprintf("show variables like '%s'", variables)
	ret, err := DB.Query(sql)
	if err != nil {
		fmt.Println(err)
	}

	type variableinfo struct {
		variableName  string
		variableValue string
	}
	for ret.Next() {
		var v variableinfo
		err := ret.Scan(&v.variableName, &v.variableValue)
		if err != nil {
			log.Fatalf("scan failed, err:%v\n", err)
		}
		value = v.variableValue
	}
	return value
}

/*  传入参数 参数值 参数作用范围(是session级别的还是global级别的)*/
func SetVariables(variables string, value string, mode string) (ifsetsuccess bool) {

	sql := fmt.Sprintf("set %s %s = '%s';", mode, variables, value)
	fmt.Println(sql)
	_, err := DB.Exec(sql)
	if err != nil {
		log.Printf("修改下游数据库SQL MODE 失败，后续可能会影响业务，建议手动修改SQL MODE为 '%s' ", VarSqlMode)
		log.Println(err)
		return false
	}

	return true
}

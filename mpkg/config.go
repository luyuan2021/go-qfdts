package mpkg

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

//结构体中字段大写开头表示可公开访问，小写表示私有（仅在定义当前结构体的包中可访问）。

type ConfigInfo struct {
	SrchostIp, SrcUser, SrcUserPasswd string
	DsthostIp, DstUser, DstUserPasswd string
	SrchostPort, DsthostPort          string
	DoDB                              []string
	IngoreDB                          []string
	ReplIgnoreDB                      []string
	IngoreUser                        []string
	MydumperConfig                    *MydumperConfig
	MyloaderConfig                    *MyloaderConfig
	Tablecountsthread                 int
}

type MydumperConfig struct {
	DumpThread   string
	Dumploglevel int
	TableRowsSplit int
}
type MyloaderConfig struct {
	LoadThread   string
	Loadloglevel int
}

func FileGetContents(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(f)
}

func GetConfig(filepath string) *ConfigInfo {

	var c ConfigInfo
	var content []byte //uint8 类型的一个slice 	fmt.Printf("%T\n",content)
	var err error

	content, err = FileGetContents(filepath)
	if err != nil {
		fmt.Println("open file error: " + err.Error())
		//return
	}
	//fmt.Println("----这是你写的配置文件的内容----\n", string(content))
	err = json.Unmarshal([]byte(content), &c)
	if err != nil {
		fmt.Println("ERROR: ", err.Error())
		//return
	}
	return &c
}

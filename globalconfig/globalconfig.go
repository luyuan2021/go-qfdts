package globalconfig

import (
	"Myqfdtsproject/mpkg"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
)

var _config *ConfigInfo

const (
	_myDumpthread = "4"
	_myLoadthread = "10"
)

type ConfigInfo struct {
	SrchostIp, SrcUser, SrcUserPasswd string
	DsthostIp, DstUser, DstUserPasswd string
	SrchostPort, DsthostPort          string
	DoDB                              []string
	IngoreDB                          []string
	IngoreUser                        []string
	ReplIgnoreDB                      []string
	MydumperConfig                    *MydumperConfig
	MyloaderConfig                    *MyloaderConfig
	TableCheckConfig                  *TableCheckConfig
}

type MydumperConfig struct {
	DumpThread   string
	Dumploglevel int
}
type MyloaderConfig struct {
	LoadThread   string
	Loadloglevel int
}
type TableCheckConfig struct {
	Tablecountsthread   string
	Tablechecksumthread string
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

	err = json.Unmarshal([]byte(content), &c)
	if err != nil {
		fmt.Println("ERROR: ", err.Error())
		//return
	}

	mpkg.PrintLog(checkConfig(&c))

	return &c
}

func checkConfig(c *ConfigInfo) string {
	mpkg.PrintLog("########## Init Config File ##########")
	if len(c.DoDB) == 0 && len(c.IngoreDB) == 0 {
		return "配置文件输入有误，指定备份的数据库名称`DoDB`和忽略备份的数据库名称`IngoreDB`不能同时为空"
	} else if len(c.DoDB) > 0 && len(c.IngoreDB) > 0 {
		return "配置文件输入有误，指定备份的数据库名称`DoDB`和忽略备份的数据库名称`IngoreDB`只能写一个"
	}
	if c.MydumperConfig.DumpThread == "" {
		c.MydumperConfig.DumpThread = _myDumpthread
		mpkg.PrintLog(fmt.Sprintf("Dumpthread is nil,using default value %s\n", _myDumpthread))
	}
	if c.MyloaderConfig.LoadThread == "" {
		c.MyloaderConfig.LoadThread = _myLoadthread
		mpkg.PrintLog(fmt.Sprintf("Loadthread is nil,using default value %s\n", _myLoadthread))
	}

	if c.TableCheckConfig.Tablecountsthread == "" {
		_tbCountthread := runtime.NumCPU() * 2

		c.TableCheckConfig.Tablecountsthread = strconv.Itoa(_tbCountthread)
		mpkg.PrintLog(fmt.Sprintf("数据行数校验并行协程数is nil,using default value %v(cpu num*2)\n", _tbCountthread))

	}
	if c.TableCheckConfig.Tablechecksumthread == "" {

		_tbCheckthread := runtime.NumCPU() * 2

		c.TableCheckConfig.Tablechecksumthread = strconv.Itoa(_tbCheckthread)
		mpkg.PrintLog(fmt.Sprintf("数据行数校验并行协程数is nil,using default value %v(cpu num*2)\n", _tbCheckthread))
	}
	return ""
}

func Cfg(filename string) *ConfigInfo {
	_config = GetConfig(filename)
	return _config
}

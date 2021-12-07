package license

import (
	"Myqfdtsproject/sys"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"
)

func Checklicense() {
	/* 解析license文件 将license文件中的生成时间和使用时间解密然后计算是否有效 */
	const (
		_validdate = 30
		// license的有效天数
		// 如果想配置成指定时间的，可以在 licenseCreate.go中 加上一个日期的方法然后将两段拼接
		// 格式比如 加密(license)WQ加密(时间段)
		// 解密的时候分开解密
		/* === 现已经支持 === */
	)

	timeObj := time.Now()
	year := timeObj.Year()
	month := timeObj.Month()

	var aliasmonth string
	if timeObj.Month() < 10 {
		aliasmonth = "0" + strconv.Itoa(int(month))
	} else {
		aliasmonth = strconv.Itoa(int(month))
	}
	var aliasday string
	/* 由于返回的Day是个int类型，所以存在一定的问题，当day是个位数时，只会表示为个位数，通过后面的函数转义出来的字符串可能存在问题 */
	day := timeObj.Day()
	if timeObj.Day() < 10 {
		aliasday = "0" + strconv.Itoa(day)
	} else {
		aliasday = strconv.Itoa(day)
	}

	formatdata := strconv.Itoa(year) + "-" + aliasmonth + "-" + aliasday
	byte_data := []byte(formatdata)
	// 将 byte 装换为 16进制的字符串
	hex_string_data := hex.EncodeToString(byte_data)
	// 将 16进制的字符串 转换 byte
	// 年-月-日 like 2021-11-17
	hex_data, _ := hex.DecodeString(hex_string_data)

	loc, _ := time.LoadLocation("Local")
	timeLayout := "2006-01-02 15:04:05" //转化所需模板
	// lic 读取 .QFUSION_DTS_LICENSE文件 然后解密 key的生成时间和有效期
	lic := sys.CurrentDirectory() + "/.QFUSION_DTS_LICENSE"
	f, _ := hex.DecodeString(strings.Split(readlicense(lic), "@Wsq12qa1Q&")[0])
	v1, _ := hex.DecodeString(strings.Split(readlicense(lic), "@Wsq12qa1Q&")[1])

	time1 := string(f) + " 00:00:00"        // license1的生成时间
	time2 := string(hex_data) + " 00:00:00" // 当前时间

	tmp1, _ := time.ParseInLocation(timeLayout, time1, loc)
	tmp2, _ := time.ParseInLocation(timeLayout, time2, loc)

	timestamp1 := tmp1.Unix()
	timestamp2 := tmp2.Unix()

	validate, _ := strconv.ParseInt(string(v1), 10, 64)

	if date := (timestamp2 - timestamp1) / 86400; date < 0 || date > validate {
		// <0 是为了防止有未来的license,>validate是license的时间期限是validate天
		log.Fatal(fmt.Sprintf("license的创建日期为: %s,license已经超期，请联系沃趣科技的同事获取license", string(f)))
	} else {
		log.Println(fmt.Sprintf("license的创建日期为: %s,剩余%v天", string(f), validate-date))
	}
}
func readlicense(file string) (lic1 string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err, "请联系沃趣科技的同事获取license")
	}
	return string(content)
}

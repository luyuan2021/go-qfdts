package license

import (
	"Myqfdtsproject/sys"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)

func Checklicense() {
	const (
		_validdate = 30
		// license的有效天数
		// 如果想配置成指定时间的，可以在 licenseCreate.go中 加上一个日期的方法然后将两段拼接
		// 格式比如 加密(license)WQ加密(时间段)
		// 解密的时候分开解密
	)
	timeObj := time.Now()
	year := timeObj.Year()
	month := timeObj.Month()
	day := timeObj.Day()
	formatdata := strconv.Itoa(year) + "-" + strconv.Itoa(int(month)) + "-" + strconv.Itoa(day)
	byte_data := []byte(formatdata)
	// 将 byte 装换为 16进制的字符串
	hex_string_data := hex.EncodeToString(byte_data)
	// 将 16进制的字符串 转换 byte
	// 年-月-日 like 2021-11-17
	hex_data, _ := hex.DecodeString(hex_string_data)

	loc, _ := time.LoadLocation("Local")
	timeLayout := "2006-01-02 00:00:00" //转化所需模板

	lic := sys.CurrentDirectory() + "/.QFUSION_DTS_LICENSE"
	f, _ := hex.DecodeString(readlicense(lic))
	time1 := string(f) + " 00:00:00"        // license1的生成时间
	time2 := string(hex_data) + " 00:00:00" // 当前时间
	//fmt.Println(time1, time2)

	tmp1, _ := time.ParseInLocation(timeLayout, time1, loc)
	tmp2, _ := time.ParseInLocation(timeLayout, time2, loc)
	timestamp1 := tmp1.Unix()
	timestamp2 := tmp2.Unix()
	if date := (timestamp2 - timestamp1) / 86400; date < 0 || date > _validdate {
		// <0 是为了防止有未来的license,>30是license的时间期限是30天
		log.Fatal(fmt.Sprintf("license的创建日期为: %s,license已经超期，请联系沃趣科技的同事获取license", string(f)))
	} else {
		log.Println(fmt.Sprintf("license的创建日期为: %s,剩余%v天", string(f), _validdate-date))
	}
}
func readlicense(file string) (lic1 string) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err, "请联系沃趣科技的同事获取license")
	}
	return string(content)
}

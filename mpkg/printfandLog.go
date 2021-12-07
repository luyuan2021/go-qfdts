package mpkg

import (
	"fmt"
	"os"
	"time"
)

type logPrint interface {
	fileLog
	printer
}

type fileLog interface {
	writeLog()
}
type printer interface {
	filePrint()
}

type logcontent struct {
	logText string
}

func (p *logcontent) writeLog() {
	//打印在屏幕上
	t := time.Now()
	header := fmt.Sprintf("%d-%d-%d %d:%d:%d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	fmt.Printf("%s %s \n", header, p.logText)

}
func (p *logcontent) filePrint() {
	//写文件方法
	s1, _ := os.Getwd()
	file, err := os.OpenFile(s1+"/dts_message.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		panic(err)
	}
	t := time.Now()
	header := fmt.Sprintf("%d-%d-%d %d:%d:%d, ", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	text := header + " " + p.logText + "\n"
	_, _ = file.WriteString(text)
	_ = file.Close()
}



func addLogContant(content string) *logcontent {
	return &logcontent{
		content,
	}
}

func PrintLog(longtext string) {
	var u logPrint
	u = addLogContant(longtext)
	u.filePrint()
	u.writeLog()
}

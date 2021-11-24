package storage

import (
	"Myqfdtsproject/files"
	"Myqfdtsproject/mpkg"
	"Myqfdtsproject/sys"
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

/* 做步骤中断的续传任务store下 */
const (
	_boltFilePath = "store"
	_boltFileName = "flag.number"
	_boltFileMode = 0666
)

var (
	flagStorePath, FlagFilePath string
	_posbefore                  string
)

type flagfile struct {
	pos int
}

type doflagfile interface {
	init()
	Get(file string) (pos string, err error)
	update(fn string, info int) bool
}

func (f *flagfile) init() {
	_ = initBolt()
}
func (f *flagfile) Get(file string) (text string, err error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("read file failed, err:", err)
		return "error", err
	}
	return string(content), nil
}

func (f *flagfile) update(fn string, info int) bool {
	_posbefore = ReadPos() //修改之前先阅读下当前的
	file, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, _boltFileMode)
	if err != nil {
		log.Printf("open file failed when update the flag file %s %s\n, err:", FlagFilePath, err)
		return false
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	_, _ = writer.WriteString(strconv.Itoa(info)) //写缓存
	_ = writer.Flush()                            //flush
	return true
}

func ReadPos() (p string) {
	ps := NewPosStorage()
	pos, _ := ps.Get(FlagFilePath)
	//mpkg.Color(101, fmt.Sprintf("HELLO WORLD%s\n", pos))
	return pos

}
func Initialize() {
	var f flagfile
	f.init()
}

func UpdatePos(pos int) {
	ps := NewPosStorage()
	if !ps.update(FlagFilePath, pos) {
		mpkg.PrintLog(fmt.Sprintf("写入flag到文件%s出错，之前的flag是%s", FlagFilePath, _posbefore))
	}
}

func NewPosStorage() doflagfile {
	return &flagfile{}

}
func initBolt() error {
	flagStorePath = filepath.Join(sys.CurrentDirectory(), _boltFilePath)
	if err := files.MkdirIfNecessary(flagStorePath); err != nil {
		return errors.New(fmt.Sprintf("create boltdb store : %s", err.Error()))
	}
	FlagFilePath = filepath.Join(flagStorePath, _boltFileName)
	files.CreateFileIfNecessary(FlagFilePath)

	return nil
}



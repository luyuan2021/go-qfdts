package mpkg

import (
	"fmt"
	"log"
	"os/exec"
)

func Cmd(cmd string, shell bool) []byte {
	if shell {
		output, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			log.Println("cmd: ",cmd," ",err)
			panic("some error found,check your command")
			Color(102,"[Error: ] some error found,check your command\n")
		}
		return output
	} else {
		output, err := exec.Command(cmd).Output()
		if err != nil {
			log.Println("cmd: ",cmd," ",err)
			panic("some error found,check your command")
			Color(102,"[Error: ] some error found,check your command\n")
		}
		fmt.Println(output)
		return output
	}

}


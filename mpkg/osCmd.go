package mpkg

import "os/exec"

func Cmd(cmd string, shell bool) []byte {
	if shell {
		output, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			panic("some error found,check your command")
		}

		return output
	} else {
		output, err := exec.Command(cmd).Output()
		if err != nil {
			panic("some error found,check your command")
		}
		return output
	}

}


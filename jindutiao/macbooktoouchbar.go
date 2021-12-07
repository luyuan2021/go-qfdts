package jindutiao

import (
	"fmt"
	"strings"
)

func TouchBar(vl int, width int) string {
	return fmt.Sprintf("%s%*c", strings.Repeat("█", vl/10), vl/10-width+1,
		([]rune(" ▏▎▍▌▋▋▊▉█"))[vl%10])
}



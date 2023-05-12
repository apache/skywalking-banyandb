package convert

import (
	"fmt"
	"strconv"
	"strings"
)

var units = map[string]int64{
	"B":   1,
	"K":   1000,
	"KB":  1000,
	"M":   1000 * 1000,
	"MB":  1000 * 1000,
	"G":   1000 * 1000 * 1000,
	"GB":  1000 * 1000 * 1000,
	"T":   1000 * 1000 * 1000 * 1000,
	"TB":  1000 * 1000 * 1000 * 1000,
	"P":   1000 * 1000 * 1000 * 1000 * 1000,
	"PB":  1000 * 1000 * 1000 * 1000 * 1000,
	"KI":  1 << 10,
	"KIB": 1 << 10,
	"MI":  1 << 20,
	"MIB": 1 << 20,
	"GI":  1 << 30,
	"GIB": 1 << 30,
	"TI":  1 << 40,
	"TIB": 1 << 40,
	"PI":  1 << 50,
	"PIB": 1 << 50,
}

// ParseSize parses a string like "1.5GB" or "1000" and returns the
// number of bytes. The following units are supported:
//
//	B, K, KB, M, MB, G, GB, T, TB, P, PB
//	KI, KIB, MI, MIB, GI, GIB, TI, TIB, PI, PIB
//
// The units are case insensitive.
func ParseSize(sizeStr string) (int64, error) {
	sep := strings.LastIndexAny(sizeStr, "01234567890. ")
	if sep == -1 {
		return -1, fmt.Errorf("invalid size: '%s'", sizeStr)
	}
	var num, sfx string
	if sizeStr[sep] != ' ' {
		num = sizeStr[:sep+1]
		sfx = sizeStr[sep+1:]
	} else {
		num = sizeStr[:sep]
		sfx = sizeStr[sep+1:]
	}
	value, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return -1, fmt.Errorf("invalid size: '%s'", sizeStr)
	}

	if len(sfx) == 0 {
		return int64(value), nil
	}
	sfx = strings.ToUpper(sfx)
	unit, ok := units[sfx]
	if !ok {
		return -1, fmt.Errorf("invalid size: '%s'", sizeStr)
	}
	return int64(value * float64(unit)), nil
}

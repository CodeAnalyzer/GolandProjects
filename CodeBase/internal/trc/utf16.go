package trc

import (
	"encoding/binary"
	"strings"
)

// decodeUTF16LEBytes декодирует UTF-16LE байты в string за один проход,
// без промежуточных []uint16 и []rune. Обрабатывает суррогатные пары
// (U+10000–U+10FFFF) по RFC 2781.
func decodeUTF16LEBytes(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	n := len(b) / 2
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		r := rune(binary.LittleEndian.Uint16(b[i*2 : i*2+2]))
		if r >= 0xD800 && r <= 0xDBFF && i+1 < n {
			r2 := rune(binary.LittleEndian.Uint16(b[(i+1)*2 : (i+1)*2+2]))
			if r2 >= 0xDC00 && r2 <= 0xDFFF {
				r = ((r-0xD800)<<10)|(r2-0xDC00) + 0x10000
				i++
			}
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

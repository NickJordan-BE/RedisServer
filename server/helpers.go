package main

import (
	"strings"
)

// Changes glob pattern to regex for ease of search
func globToRegex(glob string) string {
	var sb strings.Builder
	sb.WriteString("^")
	inBracket := false

	for _, c := range glob {
		switch c {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteByte('.')
		case '.', '+', '(', ')', '|', '$', '{', '}', '\\':
			sb.WriteString("\\" + string(c))
		case '[':
			inBracket = true
			sb.WriteByte(byte(c))
		case '^':
			if inBracket {
				sb.WriteByte(byte(c))
			} else {
				sb.WriteString("\\" + string(c))
			}
		case ']':
			inBracket = false
			sb.WriteByte(byte(c))
		default:
			sb.WriteByte(byte(c))
		}
	}

	sb.WriteString("$")
	return sb.String()
}

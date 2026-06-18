package backend

import "strings"

// shellSafe is the set of characters a token may contain and still pass through
// a POSIX shell unquoted; it mirrors Python's shlex.quote allowlist.
const shellSafe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@%+=:,./-_"

// ShellQuote renders s as a single POSIX-shell token, escaping each embedded
// single quote by closing the quote, emitting an escaped quote, and reopening.
func ShellQuote(s string) string {
	if s == "" {
		return "''"
	}
	for _, r := range s {
		if !strings.ContainsRune(shellSafe, r) {
			return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
		}
	}
	return s
}

// quoteAll renders each token through ShellQuote, preserving order.
func quoteAll(tokens []string) []string {
	quoted := make([]string, len(tokens))
	for i, tok := range tokens {
		quoted[i] = ShellQuote(tok)
	}
	return quoted
}

// fishQuote renders s as a single fish token. fish single quotes treat \\ and \'
// as escapes (unlike POSIX), so backslashes and quotes are backslash-escaped in
// place; backslashes are escaped first so the quote escapes are not doubled.
func fishQuote(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", `\'`) + "'"
}

// wrapBashLogin renders command as a single `bash -lc <line>` string for the
// superset terminal's --command. Two shells parse it: the terminal's login shell
// (fish) parses the whole string and must receive the inner line fish-quoted,
// while bash -lc reparses that line and needs each token POSIX-quoted.
func wrapBashLogin(command []string) string {
	return "bash -lc " + fishQuote(strings.Join(quoteAll(command), " "))
}

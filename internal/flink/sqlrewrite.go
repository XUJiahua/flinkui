package flink

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// This file implements the structured SQL rewrite used by the replay-from-
// timestamp feature: it locates a job's Kafka source CREATE TABLE and upserts
// only the three startup keys in its WITH(...) block, leaving everything else
// intact. See docs/replay-from-timestamp-design.md §4.3. The whole original SQL
// is backed up before any edit, so revert restores it verbatim (this rewriter
// is not used on the revert path).

const (
	keyScanStartupMode   = "scan.startup.mode"
	keyScanStartupMillis = "scan.startup.timestamp-millis"
	keyAutoOffsetReset   = "properties.auto.offset.reset"
)

// withBlock locates one WITH(...) options block and its owning table.
type withBlock struct {
	table     string
	connector string
	open      int // index of '(' after WITH
	close     int // index of the matching ')'
}

var createTableRe = regexp.MustCompile(`(?i)\bCREATE\s+(?:TEMPORARY\s+)?TABLE\s+` + "`?" + `([A-Za-z0-9_]+)` + "`?")

// findWithBlocks returns each table's WITH block, quote- and comment-aware.
func findWithBlocks(sql string) []withBlock {
	var out []withBlock
	locs := createTableRe.FindAllStringSubmatchIndex(sql, -1)
	for _, loc := range locs {
		table := sql[loc[2]:loc[3]]
		open := findWithOpenParen(sql, loc[1])
		if open < 0 {
			continue
		}
		closeIdx := matchParen(sql, open)
		if closeIdx < 0 {
			continue
		}
		body := sql[open+1 : closeIdx]
		out = append(out, withBlock{
			table:     table,
			connector: optionValue(body, "connector"),
			open:      open,
			close:     closeIdx,
		})
	}
	return out
}

// findWithOpenParen returns the index of the '(' of the first WITH block at or
// after start, skipping quotes/comments. Returns -1 if not found.
func findWithOpenParen(sql string, start int) int {
	i := start
	for i < len(sql) {
		i = skipQuotesAndComments(sql, i)
		if i >= len(sql) {
			return -1
		}
		if (sql[i] == 'w' || sql[i] == 'W') && matchesKeyword(sql, i, "WITH") {
			j := i + 4
			for j < len(sql) && isSpace(sql[j]) {
				j++
			}
			if j < len(sql) && sql[j] == '(' {
				return j
			}
			i = j
			continue
		}
		// A semicolon ends a statement without a WITH block.
		if sql[i] == ';' {
			return -1
		}
		i++
	}
	return -1
}

// matchParen returns the index of the ')' matching the '(' at open, quote- and
// comment-aware. Returns -1 if unbalanced.
func matchParen(sql string, open int) int {
	depth := 0
	i := open
	for i < len(sql) {
		i = skipQuotesAndComments(sql, i)
		if i >= len(sql) {
			return -1
		}
		switch sql[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

// skipQuotesAndComments advances i past a single-quoted string literal, a
// line comment (-- ... \n) or a block comment (/* ... */) starting at i; if i is
// not at any of these it returns i unchanged.
func skipQuotesAndComments(sql string, i int) int {
	if i >= len(sql) {
		return i
	}
	switch {
	case sql[i] == '\'':
		i++
		for i < len(sql) {
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' { // escaped ''
					i += 2
					continue
				}
				return i + 1
			}
			i++
		}
		return i
	case sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-':
		for i < len(sql) && sql[i] != '\n' {
			i++
		}
		return i
	case sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*':
		i += 2
		for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
			i++
		}
		return i + 2
	}
	return i
}

func matchesKeyword(sql string, i int, kw string) bool {
	if i+len(kw) > len(sql) {
		return false
	}
	if !strings.EqualFold(sql[i:i+len(kw)], kw) {
		return false
	}
	// Word boundaries.
	if i > 0 && isWordByte(sql[i-1]) {
		return false
	}
	if i+len(kw) < len(sql) && isWordByte(sql[i+len(kw)]) {
		return false
	}
	return true
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}
func isSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

// optionRe matches a single-quoted 'key' = 'value' option pair (value may
// contain doubled ” escapes). It is applied to comment-stripped block bodies.
var optionRe = regexp.MustCompile(`'([^']+)'\s*=\s*'((?:[^']|'')*)'`)

// optionValue returns the value of a WITH option by key (empty if absent).
func optionValue(body, key string) string {
	for _, m := range optionRe.FindAllStringSubmatch(stripComments(body), -1) {
		if m[1] == key {
			return m[2]
		}
	}
	return ""
}

// stripComments removes -- line and /* */ block comments from a snippet so
// option parsing never matches a commented-out pair.
func stripComments(s string) string {
	var b strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\'' {
			j := skipQuotesAndComments(s, i)
			b.WriteString(s[i:j])
			i = j
			continue
		}
		if (s[i] == '-' && i+1 < len(s) && s[i+1] == '-') ||
			(s[i] == '/' && i+1 < len(s) && s[i+1] == '*') {
			i = skipQuotesAndComments(s, i)
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// rewriteForReplay locates the Kafka source table and upserts the timestamp
// startup keys in its WITH block. sourceTable may be empty when there is exactly
// one Kafka source. Returns the new SQL and the table that was rewritten.
func rewriteForReplay(sql, sourceTable string, timestampMillis int64, autoOffsetReset string) (string, string, error) {
	if autoOffsetReset == "" {
		autoOffsetReset = "earliest"
	}
	blocks := findWithBlocks(sql)
	var kafka []withBlock
	for _, b := range blocks {
		if strings.HasPrefix(strings.ToLower(b.connector), "kafka") {
			kafka = append(kafka, b)
		}
	}
	if len(kafka) == 0 {
		return "", "", fmt.Errorf("no Kafka source table found in SQL")
	}
	var target withBlock
	switch {
	case sourceTable != "":
		found := false
		for _, b := range kafka {
			if b.table == sourceTable {
				target, found = b, true
				break
			}
		}
		if !found {
			return "", "", fmt.Errorf("Kafka source table %q not found", sourceTable)
		}
	case len(kafka) == 1:
		target = kafka[0]
	default:
		names := make([]string, len(kafka))
		for i, b := range kafka {
			names[i] = b.table
		}
		return "", "", fmt.Errorf("multiple Kafka source tables (%s); specify sourceTable", strings.Join(names, ", "))
	}

	body := sql[target.open+1 : target.close]
	newBody := upsertWithOptions(body, [][2]string{
		{keyScanStartupMode, "timestamp"},
		{keyScanStartupMillis, strconv.FormatInt(timestampMillis, 10)},
		{keyAutoOffsetReset, autoOffsetReset},
	})
	return sql[:target.open+1] + newBody + sql[target.close:], target.table, nil
}

// upsertWithOptions parses the option pairs in a WITH block body (comments
// stripped) and upserts the given key/values, preserving original order for
// existing keys and appending new ones. It re-renders the block with a
// consistent two-space indent.
func upsertWithOptions(body string, upserts [][2]string) string {
	indent := detectIndent(body)

	type kv struct{ k, v string }
	var pairs []kv
	seen := map[string]int{}
	for _, m := range optionRe.FindAllStringSubmatch(stripComments(body), -1) {
		seen[m[1]] = len(pairs)
		pairs = append(pairs, kv{m[1], m[2]})
	}
	for _, up := range upserts {
		if idx, ok := seen[up[0]]; ok {
			pairs[idx].v = up[1]
		} else {
			seen[up[0]] = len(pairs)
			pairs = append(pairs, kv{up[0], up[1]})
		}
	}

	var b strings.Builder
	b.WriteString("\n")
	for i, p := range pairs {
		b.WriteString(indent)
		b.WriteString("'")
		b.WriteString(p.k)
		b.WriteString("' = '")
		b.WriteString(p.v)
		b.WriteString("'")
		if i < len(pairs)-1 {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// detectIndent returns the leading whitespace of the first option line, or two
// spaces as a default.
func detectIndent(body string) string {
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		trimmed := strings.TrimLeft(line, " \t")
		return line[:len(line)-len(trimmed)]
	}
	return "  "
}

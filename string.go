package inmemdb

import "strings"

type String string

// ModelSortable interface
func (u String) ModelLess(ms ModelSortable) bool {
	mv := string(ms.(String))
	return strings.Compare(string(u), mv) < 0
}
func (u String) ModelEqual(ms ModelSortable) bool {
	mv := string(ms.(String))
	return strings.Compare(string(u), mv) == 0
}

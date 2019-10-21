package inmemdb

import (
	"fmt"
	"strings"
)

type RelationType int

const (
	RelationTypeNotRelation RelationType = iota
	RelationTypeHasMany
	RelationTypeManyToMany
	RelationTypeBelongsTo
	RelationTypeHasOne
)

// Структура, описывающая вид связей в базе данных.
type Relation struct {
	Type                RelationType
	ForeignKey          string
	ManyToManyTableName string
	Preload             bool
	Cascade             bool
}

// Строковое представление структуры Relation для отладки.
func (r Relation) String() string {
	var preload string
	if r.Preload {
		preload = " preload"
	}

	switch r.Type {
	case RelationTypeNotRelation:
		return "-"
	case RelationTypeHasOne:
		return fmt.Sprintf("HasOne (FK: %s)%s", r.ForeignKey, preload)
	case RelationTypeManyToMany:
		return fmt.Sprintf("Many2Many (FK: %s)%s", r.ManyToManyTableName, preload)
	case RelationTypeHasMany:
		return fmt.Sprintf("HasMany (FK: %s)%s", r.ForeignKey, preload)
	case RelationTypeBelongsTo:
		return fmt.Sprintf("BelongsTo%s", preload)
	}

	return "<Unknown Relation>"
}

// ParseStoreTag - разбор тега store.Tag
// возвращает false если данное отношение нужно игнорировать
func (r *Relation) ParseTag(tag string) bool {
	options := strings.Split(tag, ",")
	for _, option := range options {
		switch strings.TrimSpace(option) {
		case TagOptionIgnore:
			return false
		case TagOptionCascade:
			r.Cascade = true
		default:
			if !strings.Contains(option, ":") {
				continue
			}

			kv := strings.Split(option, ":")
			k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])

			switch k {
			case TagOptionForeignKey:
				r.ForeignKey = v
			case TagOptionManyToMany:
				r.ManyToManyTableName = v
			}
		}
	}
	return true
}

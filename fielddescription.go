package inmemdb

import (
	"fmt"
	"reflect"
)

// FieldDescription - describe a field of model
type FieldDescription struct {
	Idx           int                 // index in ModelDescription.Columns
	StructField   reflect.StructField // reflect field
	ElemType      reflect.Type        // type of field
	Name          string              // store name
	JsonName      string
	JsonOmitEmpty bool
	Nullable      bool
	Skip          bool
	IsForeignKey  bool
	RelatedColumn *FieldDescription

	Relation Relation
}

func (fd FieldDescription) String() string {
	ret := fd.StructField.Name

	if fd.Skip {
		return "- " + ret + " [skip]"
	}

	nullable := ""
	if fd.Nullable {
		nullable = "*"
	}

	ret = fmt.Sprintf("%s%s (store: %s)", nullable, ret, fd.Name)

	if fd.IsForeignKey {
		ret += " FK"
	}

	if fd.RelatedColumn != nil {
		ret += fmt.Sprintf(" [rel.: %s (%s)]", fd.StructField.Name, fd.Name)
	}

	if fd.Relation.Type != RelationTypeNotRelation {
		return "- " + ret + fmt.Sprintf(" <%s>", fd.Relation)
	}

	return ret
}

func (fd *FieldDescription) IsStored() bool {
	return !fd.Skip && fd.Relation.Type == RelationTypeNotRelation
}

func (fd *FieldDescription) MarshalJSON() ([]byte, error) {
	if fd == nil {
		return []byte("null"), nil
	}
	return []byte(fd.JsonName), nil
}

func (fd *FieldDescription) MarshalText() (text []byte, err error) {
	if fd == nil {
		return []byte("null"), nil
	}
	return []byte(fd.JsonName), nil
}

func GetFieldValueByName(val reflect.Value, name string) (reflect.Value, error) {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		if !typ.Field(i).Anonymous {
			continue
		}

		subVal := reflect.Indirect(val.Field(i))
		if subVal.Kind() != reflect.Struct {
			continue
		}

		if field, err := GetFieldValueByName(subVal, name); err == nil {
			return field, nil
		}
	}

	if _, ok := typ.FieldByName(name); ok {
		return val.FieldByName(name), nil
	}

	return reflect.Value{}, fmt.Errorf("no '%s' field", name)
}

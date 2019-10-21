package inmemdb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

const (
	TagValidate = "validate"

	Tag     = "store"
	TagName = "db"

	TagOptionIgnore     = "-"
	TagOptionCascade    = "cascade"
	TagOptionForeignKey = "foreignKey"
	TagOptionManyToMany = "many2many"

	IDField        = "ID"
	CreatedAtField = "CreatedAt"
	UpdatedAtField = "UpdatedAt"
	DeletedAtField = "DeletedAt"
)

type Storable interface {
	StoreName() string
}

// Model description
type ModelDescription struct {
	ModelType reflect.Type
	StoreName string

	IdField        *FieldDescription
	CreatedAtField *FieldDescription
	UpdatedAtField *FieldDescription
	DeletedAtField *FieldDescription

	Columns           []FieldDescription
	ColumnPtrs        []*FieldDescription
	ColumnByName      map[string]*FieldDescription //указатель
	ColumnByFieldName map[string]*FieldDescription
	ColumnByJsonName  map[string]*FieldDescription
}

func (md ModelDescription) GetModelType() reflect.Type {
	return md.ModelType
}

func (md ModelDescription) GetName() string {
	return md.ModelType.String()
}

func GetUniqTypeName(typ reflect.Type) string {
	return fmt.Sprintf("%s.%s", typ.PkgPath(), typ.Name())
}

func (md ModelDescription) GetUniqName() string {
	return GetUniqTypeName(md.ModelType)
}

func (md ModelDescription) GetColumnByFieldName(fieldName string) (*FieldDescription, error) {
	field, ok := md.ColumnByFieldName[fieldName]
	if !ok {
		return nil, fmt.Errorf("no such field: %s.%s", md.ModelType.Name(), fieldName)
	}
	return field, nil
}

func (md ModelDescription) GetColumnsByFieldNames(fieldNames ...string) (res []*FieldDescription) {
	for _, fieldName := range fieldNames {
		field, ok := md.ColumnByFieldName[fieldName]
		if !ok {
			panic(fmt.Sprintf("no such field: %s.%s", md.ModelType.Name(), fieldName))
		}
		res = append(res, field)
	}
	return
}

func (md ModelDescription) GetStoredColumnNames() (res []string) {
	cols := make([]string, 0, len(md.Columns))
	for _, fd := range md.ColumnPtrs {
		if !fd.IsStored() {
			continue
		}
		cols = append(cols, fd.Name)
	}
	return cols
}

func (md ModelDescription) GetColumnByJsonName(jsonName string) (*FieldDescription, error) {
	field, ok := md.ColumnByJsonName[jsonName]
	if !ok {
		return nil, fmt.Errorf("no such field: %s.%s", md.ModelType.Name(), jsonName)
	}
	return field, nil
}

func GetFieldByName(typ reflect.Type, name string) (reflect.StructField, error) {
	for i := 0; i < typ.NumField(); i++ {
		structField := typ.Field(i)
		if !structField.Anonymous {
			continue
		}

		var subTyp reflect.Type

		switch structField.Type.Kind() {
		case reflect.Struct:
			subTyp = structField.Type
		case reflect.Ptr:
			elem := structField.Type.Elem()
			if elem.Kind() == reflect.Struct {
				subTyp = elem
			}
		}

		if subTyp != nil {
			if field, err := GetFieldByName(subTyp, name); err == nil {
				return field, nil
			}
		}
	}

	if field, ok := typ.FieldByName(name); ok {
		return field, nil
	}

	return reflect.StructField{}, fmt.Errorf("no '%s' field", name)
}

func JsonFieldName(field reflect.StructField) string {
	jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
	if jsonTag == "-" {
		return ""
	} else if len(jsonTag) == 0 {
		return field.Name
	}
	return jsonTag
}

func (md *ModelDescription) init() error {
	columns := make([]FieldDescription, 0, md.ModelType.NumField())
	columnByName := make(map[string]*FieldDescription)
	columnByJsonName := make(map[string]*FieldDescription)
	columnByFieldName := make(map[string]*FieldDescription)

	if err := fillColumns(md.ModelType, &columns); err != nil {
		return err
	}

	var idField reflect.StructField
	if field, err := GetFieldByName(md.ModelType, IDField); err != nil {
		return err
	} else {
		idField = field
	}

	for i := range columns {
		column := &columns[i]
		structField := column.StructField
		fieldName := structField.Name

		// fill shortcuts
		if _, ok := columnByName[column.Name]; ok {
			return fmt.Errorf("column name not uniq: '%s'", column.Name)
		}
		columnByName[column.Name] = column

		columnByFieldName[column.StructField.Name] = column
		if jsonName := JsonFieldName(column.StructField); len(jsonName) > 0 {
			columnByJsonName[jsonName] = column
		} else {
			columnByJsonName[column.StructField.Name] = column
		}

		if !column.Relation.ParseTag(structField.Tag.Get(Tag)) {
			continue
		}

		fkName := column.Relation.ForeignKey
		if fkName == "" {
			fkName = md.ModelType.Name() + IDField
		}

		switch structField.Type.Kind() {
		case reflect.Slice:
			elemType := structField.Type.Elem()
			if elemType.Kind() == reflect.Ptr {
				elemType = elemType.Elem()
			}

			if elemType.Kind() == reflect.Struct {
				if _, err := GetFieldByName(elemType, fkName); err == nil {
					column.Relation.Type = RelationTypeHasMany
					column.Relation.ForeignKey = fkName
					if elemType == md.ModelType {
						column.RelatedColumn = columnByFieldName[fkName]
					}

				} else if len(column.Relation.ManyToManyTableName) > 0 {
					column.Relation.Type = RelationTypeManyToMany
				}
			}

		case reflect.Struct, reflect.Ptr:
			sfType := structField.Type
			if sfType.Kind() == reflect.Ptr {
				sfType = sfType.Elem()
				if sfType.Kind() != reflect.Struct {
					continue
				}
			}

			if !sfType.Implements(reflect.TypeOf((*Storable)(nil)).Elem()) {
				continue
			}

			var sfIdField reflect.StructField
			if field, err := GetFieldByName(sfType, IDField); err != nil {
				return err
			} else {
				sfIdField = field
			}

			if fkField, err := GetFieldByName(md.ModelType, fieldName+IDField); err == nil {

				if fkField.Type != sfIdField.Type && fkField.Type != reflect.PtrTo(sfIdField.Type) {
					return fmt.Errorf(
						"field type (%s.%s/%s.%s) mismatch: %s/%s",
						md.ModelType.Name(),
						fkField.Name,
						sfType.Name(),
						sfIdField.Name,
						fkField.Type.Name(),
						sfIdField.Type.Name())
				}

				fkColumn := columnByFieldName[fkField.Name]
				fkColumn.IsForeignKey = true
				fkColumn.RelatedColumn = column

				column.Relation.Type = RelationTypeBelongsTo
				column.RelatedColumn = fkColumn

			} else if fkField, err := GetFieldByName(sfType, fkName); err == nil {

				if fkField.Type != idField.Type && fkField.Type != reflect.PtrTo(idField.Type) {
					return fmt.Errorf(
						"field type (%s.%s/%s.%s) mismatch: %s/%s",
						md.ModelType.Name(),
						idField.Name,
						sfType.Name(),
						fkField.Name,
						idField.Type.Name(),
						fkField.Type.Name())
				}

				column.Relation.Type = RelationTypeHasOne
				column.Relation.ForeignKey = fkName
				if sfType == md.ModelType {
					column.RelatedColumn = columnByFieldName[fkName]
				}

			}
		}
	}

	md.ColumnPtrs = make([]*FieldDescription, len(columns))
	// should not be in previous loop as it can return
	for i := range columns {
		column := &columns[i]
		column.Idx = i
		md.ColumnPtrs[i] = column

		switch column.StructField.Name {
		case IDField:
			md.IdField = column
		case CreatedAtField:
			md.CreatedAtField = column
		case UpdatedAtField:
			md.UpdatedAtField = column
		case DeletedAtField:
			md.DeletedAtField = column
		}
	}

	// set relatated column for recursive models (ex.: ID, related column would be ParentID)
	if md.IdField != nil {
		for i := range columns {
			column := &columns[i]
			switch column.Relation.Type {
			case RelationTypeHasOne, RelationTypeHasMany:
				if column.RelatedColumn != nil {
					md.IdField.RelatedColumn = column.RelatedColumn
					break // first one
				}
			}
		}
	}

	md.Columns = columns
	md.ColumnByName = columnByName
	md.ColumnByFieldName = columnByFieldName
	md.ColumnByJsonName = columnByJsonName

	return nil
}

// Создает новую модель и заполняет поля
func NewModelDescription(modelType reflect.Type, storeName string) (*ModelDescription, error) {
	modelDescription := ModelDescription{
		ModelType: modelType,
		StoreName: storeName,
	}

	if err := modelDescription.init(); err != nil {
		return nil, fmt.Errorf("init ModelDescription failed: %s", err)
	}

	return &modelDescription, nil
}

func fillColumns(typ reflect.Type, columns *[]FieldDescription) error {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < typ.NumField(); i++ {
		structField := typ.Field(i)
		if len(structField.PkgPath) > 0 {
			continue
		}

		if structField.Anonymous {
			if err := fillColumns(structField.Type, columns); err != nil {
				return err
			}
			continue
		}

		fieldName := structField.Name

		// только так, т.к. эта же функция будет использоваться для распознавания имен полей БД
		name := sqlx.NameMapper(fieldName)
		dbtag := structField.Tag.Get(TagName)
		if len(dbtag) > 0 {
			if dbtag == "-" {
				continue
			}
			name = dbtag
		}

		elemType := structField.Type
		switch elemType.Kind() {
		case reflect.Ptr, reflect.Slice:
			elemType = elemType.Elem()
		}

		column := FieldDescription{
			StructField:   structField,
			ElemType:      elemType,
			Name:          name,
			JsonName:      JsonFieldName(structField),
			JsonOmitEmpty: strings.Contains(structField.Tag.Get("json"), "omitempty"),
			Skip:          structField.Tag.Get(Tag) == "-",
			Nullable:      structField.Type.Kind() == reflect.Ptr,
		}

		column.Skip = structField.Tag.Get(Tag) == "-"

		*columns = append(*columns, column)
	}

	return nil
}

func (md *ModelDescription) SearchField(f *FieldDescription) int {
	for i, p := range md.ColumnPtrs {
		if p == f {
			return i
		}
	}
	return -1
}

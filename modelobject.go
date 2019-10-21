package inmemdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/jmoiron/sqlx"
	"gopkg.in/go-playground/validator.v9"
)

type ModelObject struct {
	md *ModelDescription
	v  []interface{}

	aliasbuf [4]byte

	lastColScanner sqlx.ColScanner
	cols           []string
	fds            []*FieldDescription
	lastAlias      string
}

var valPool = sync.Pool{}

func getValSlice(c int) []interface{} {
	sl := valPool.Get()
	if sl != nil {
		vsl := sl.([]interface{})
		if cap(vsl) >= c {
			vsl = vsl[:c]
			for i := range vsl {
				vsl[i] = nil
			}
			return vsl
		}
	}
	return make([]interface{}, c)
}

func putValSlice(sl []interface{}) {
	if sl == nil {
		return
	}
	valPool.Put(sl[:0])
}

var bufferPool = sync.Pool{}

func GetBuffer() *bytes.Buffer {
	b := bufferPool.Get()
	if b != nil {
		vb := b.(*bytes.Buffer)
		return vb
	}
	return &bytes.Buffer{}
}

func PutBuffer(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	bufferPool.Put(b)
}

func NewModelObject(md *ModelDescription, sessionFrom ...ModelObject) ModelObject {
	r := ModelObject{
		md: md,
		v:  getValSlice(len(md.ColumnPtrs)),
	}
	if len(sessionFrom) > 0 {
		from := sessionFrom[0]
		r.lastColScanner = from.lastColScanner
		r.cols = from.cols
		r.fds = from.fds
		r.lastAlias = from.lastAlias
	}
	return r
}

func (mo ModelObject) String() string {
	b, _ := json.Marshal(mo)
	return string(b)
}

func (mo ModelObject) MD() *ModelDescription {
	return mo.md
}

func (mo *ModelObject) Close() {
	putValSlice(mo.v)
	mo.v = nil
	mo.cols = nil
	mo.fds = nil
	mo.lastColScanner = nil
	mo.md = nil
	mo.lastAlias = ""
}

func (mo ModelObject) Clear() {
	for i := range mo.v {
		mo.v[i] = nil
	}
}

type NullType struct{}

func (fnil NullType) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (fnil *NullType) ConvertFrom(v interface{}) error {
	return nil
}

var Null NullType

func (mo ModelObject) MarshalJSON() ([]byte, error) {
	b := GetBuffer()

	b.Grow(len(mo.v) * 32)
	enc := json.NewEncoder(b)
	b.WriteByte('{')
	comma := false
	for fdi, v := range mo.v {
		fd := mo.md.ColumnPtrs[fdi]
		_, isnull := v.(NullType)
		if v == nil || fd.JsonName == "" {
			continue
		}

		if fd.JsonOmitEmpty && (isnull || reflect.ValueOf(v).IsZero()) {
			continue
		}

		if comma {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(fd.JsonName)
		b.WriteByte('"')
		b.WriteByte(':')
		if isnull {
			b.WriteString("null")
		} else {
			enc.Encode(v)
		}
		comma = true
	}
	b.WriteByte('}')
	res := b.Bytes()
	PutBuffer(b)

	return res, nil
}

// keys = json names
func (mo *ModelObject) FromMap(data map[string]interface{}) error {
	for k, v := range data {
		fd, ok := mo.md.ColumnByJsonName[k]
		if !ok {
			continue
		}

		if fd.Relation.Type != RelationTypeNotRelation {
			continue
		} else if fd == mo.md.CreatedAtField || fd == mo.md.UpdatedAtField || fd == mo.md.DeletedAtField {
			// internal field
			continue
		}

		cv, err := ConvertToType(v, fd.StructField.Type)
		if err != nil {
			return fmt.Errorf("can't convert json field %s to %s: %s", fd.JsonName, fd.StructField.Type, err)
		}

		var newv reflect.Value
		if cv == nil {
			newv = reflect.Zero(fd.StructField.Type)
		} else {
			newv = reflect.ValueOf(cv)
		}

		mo.v[fd.Idx] = newv.Interface()
	}

	return nil
}

func (mo ModelObject) Prepare() error {
	return mo.Validate()
}

func (mo *ModelObject) UnmarshalJSON(b []byte) error {
	if bytes.EqualFold(b, []byte("null")) {
		mo.Clear()
		return nil
	}
	data := make(map[string]interface{})
	err := json.Unmarshal(b, &data)
	if err != nil {
		return err
	}
	return mo.FromMap(data)
}

func (mo ModelObject) Validate() error {
	var validationErrors ErrorValidations

	for fdi, v := range mo.v {
		fd := mo.md.ColumnPtrs[fdi]
		if v == nil {
			continue
		}
		if err := valid.Var(v, fd.StructField.Tag.Get(TagValidate)); err != nil {
			if verrs, ok := err.(validator.ValidationErrors); ok {
				for _, verr := range verrs {
					validationErrors = append(validationErrors, ErrorValidation{
						Type:             mo.md.ModelType,
						FieldDescription: *fd,
						Validator:        verr.ActualTag(),
						Param:            verr.Param(),
					})
				}
				continue // we accumulate all validation errors
			}
			return err
		}
	}

	if len(validationErrors) > 0 {
		idValue := mo.v[mo.md.IdField.Idx]
		if idValue != nil {
			for i := range validationErrors {
				(&validationErrors[i]).ID = idValue
			}
		}
		return validationErrors
	}

	return nil
}

var holePool = sync.Pool{}

func getHoleSlice(c int) []interface{} {
	sl := holePool.Get()
	if sl != nil {
		vsl := sl.([]interface{})
		if cap(vsl) >= c {
			return vsl[:c]
		}
	}
	vsl := make([]interface{}, c)
	for i := range vsl {
		vsl[i] = &GreyHole{}
	}
	return vsl
}

func putHoleSlice(sl []interface{}) {
	if sl == nil {
		return
	}
	holePool.Put(sl[:0])
}

func ByteSlice2String(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}

// RowScan scans a single sqlx.Row into the dest ModelObject.
// Columns which occur more than once in the result will overwrite
// each other!
// Only one table alias (or none) is supported
func (mo *ModelObject) RowScan(r sqlx.ColScanner, aliases ...string) error {
	if mo.lastColScanner != r {
		columns, err := r.Columns()
		if err != nil {
			return err
		}
		mo.cols = columns
		mo.fds = make([]*FieldDescription, len(columns))
		if len(aliases) == 0 {
			for i, column := range columns {
				ip := strings.IndexByte(column, '.')
				if ip >= 0 {
					column = column[ip+1:]
				}

				mo.fds[i] = nil
				if fd, ok := mo.md.ColumnByName[column]; ok {
					mo.fds[i] = fd
				}
			}
		}
		mo.lastColScanner = r
	}

	if len(aliases) >= 1 && aliases[0] != mo.lastAlias {
		alias := aliases[0]
		pa := alias
		if len(alias) < len(mo.aliasbuf) {
			copy(mo.aliasbuf[:len(alias)], alias)
			mo.aliasbuf[len(alias)] = '.'
			pa = ByteSlice2String(mo.aliasbuf[:len(alias)+1])
		} else {
			pa += "."
		}

		for i, column := range mo.cols {
			mo.fds[i] = nil
			if strings.HasPrefix(column, pa) {
				column = column[len(pa):]
				if fd, ok := mo.md.ColumnByName[column]; ok {
					mo.fds[i] = fd
				}
			}
		}
		mo.lastAlias = alias
	}

	values := getHoleSlice(len(mo.cols))

	for i := range values {
		if mo.fds[i] == nil {
			values[i].(*GreyHole).T = nil
		} else {
			values[i].(*GreyHole).T = mo.fds[i].StructField.Type
		}
	}

	err := r.Scan(values...)
	if err != nil {
		return err
	}

	for i, fd := range mo.fds {
		if fd == nil {
			continue
		}

		v := values[i].(*GreyHole)

		if (!v.Valid) && fd == mo.md.IdField {
			mo.Clear()
			break
		}

		if v.V == nil {
			mo.v[fd.Idx] = Null
		} else {
			mo.v[fd.Idx] = v.V
		}
	}

	putHoleSlice(values)

	return r.Err()
}

func (mo ModelObject) Field(fd *FieldDescription) interface{} {
	return mo.v[fd.Idx]
}

func (mo ModelObject) Delete(fd *FieldDescription) {
	mo.v[fd.Idx] = nil
}

func (mo ModelObject) IDField() interface{} {
	return mo.Field(mo.md.IdField)
}

func (mo ModelObject) SetIDField(id interface{}) error {
	if mo.md.IdField.StructField.Type == reflect.TypeOf(id) {
		mo.v[mo.md.IdField.Idx] = id
		return nil
	}
	cv, err := ConvertToType(id, mo.md.IdField.StructField.Type)
	if err != nil {
		return fmt.Errorf("can't convert field 'id' value %#v to %s: %w", id, mo.md.IdField.StructField.Type, err)
	}
	mo.v[mo.md.IdField.Idx] = cv
	return nil
}

func (mo ModelObject) FieldCount() int {
	cnt := 0
	for _, v := range mo.v {
		if v != nil {
			cnt++
		}
	}
	return cnt
}

func (mo ModelObject) SetField(fd *FieldDescription, val interface{}) error {
	_, ok1 := val.(ModelObject)
	_, ok2 := val.(*ModelObject)
	if fd.StructField.Type == reflect.TypeOf(val) || ok1 || ok2 || !fd.IsStored() {
		mo.v[fd.Idx] = val
		return nil
	}
	cv, err := ConvertToType(val, fd.StructField.Type)
	if err != nil {
		return fmt.Errorf("can't convert field '%s' value %#v to %s: %w", fd.Name, val, fd.StructField.Type, err)
	}
	mo.v[fd.Idx] = cv
	return nil
}

func (mo ModelObject) DBData() (cols []string, vals []interface{}) {
	ln := mo.FieldCount()
	cols = make([]string, 0, ln)
	vals = make([]interface{}, 0, ln)
	for fdi, v := range mo.v {
		fd := mo.md.ColumnPtrs[fdi]
		if v == nil || !fd.IsStored() {
			continue
		}
		cols = append(cols, fd.Name)
		vals = append(vals, v)
	}
	return
}

func (mo ModelObject) CopyTo(dest *ModelObject) {
	for fdi, v := range mo.v {
		dest.v[fdi] = v
	}
}

func (mo ModelObject) Walk(f func(fd *FieldDescription, value interface{})) {
	for fdi, v := range mo.v {
		fd := mo.md.ColumnPtrs[fdi]
		if v != nil {
			f(fd, v)
		}
	}
}

func (mo *ModelObject) FromStruct(src interface{}) error {
	ret := reflect.Indirect(reflect.ValueOf(src))
	if ret.Kind() != reflect.Struct {
		return fmt.Errorf("source must be a struct")
	}

	for _, fd := range mo.md.ColumnPtrs {
		if !fd.IsStored() {
			continue
		}
		value, err := GetFieldValueByName(ret, fd.StructField.Name)
		if err != nil {
			return err
		}
		mo.v[fd.Idx] = value.Interface()
	}

	return nil
}

// target is pointer to model struct
func (mo ModelObject) ToStruct(target interface{}) error {
	ret := reflect.ValueOf(target)
	if ret.Kind() != reflect.Ptr || ret.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("target must be a pointer to struct")
	}

	for fdi, v := range mo.v {
		fd := mo.md.ColumnPtrs[fdi]
		if v == nil {
			continue
		}
		value, err := GetFieldValueByName(ret.Elem(), fd.StructField.Name)
		if err != nil {
			return err
		}

		var newValue reflect.Value
		_, isnull := v.(NullType)
		if v == nil || isnull {
			newValue = reflect.Zero(fd.StructField.Type)
		} else {
			newValue = reflect.ValueOf(v)
		}

		if value.Kind() != newValue.Kind() {

			elem := func(val reflect.Value) reflect.Value {
				if val.Kind() != reflect.Ptr {
					return val
				}

				if val.IsNil() {
					val.Set(reflect.New(val.Type().Elem()))
				}

				return val.Elem()
			}

			value = elem(value)
			newValue = elem(newValue)
		}

		switch {
		case newValue.Type() == value.Type():
			value.Set(newValue)
		case newValue.Type().ConvertibleTo(value.Type()):
			value.Set(newValue.Convert(value.Type()))
		default:
			return fmt.Errorf(
				"can't set value for field %s: unconvertible types %s and %s",
				fd.StructField.Name,
				value.Type().Name(),
				newValue.Type().Name())
		}
	}

	return nil
}

func (mo ModelObject) GetColumnsByFieldNames(fieldNames ...string) (res []*FieldDescription) {
	return mo.md.GetColumnsByFieldNames(fieldNames...)
}

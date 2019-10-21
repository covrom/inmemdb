package inmemdb

import (
	"database/sql/driver"
	"reflect"

	"gopkg.in/go-playground/validator.v9"
)

var valid *validator.Validate // валидатор

func init() {
	valid = validator.New()
	valid.RegisterCustomTypeFunc(
		ValidateValuer,
		UUIDv4{},
	)
}

func ValidateValuer(field reflect.Value) interface{} {
	switch v := field.Interface().(type) {
	case UUIDv4:
		if !v.IsZero() {
			val, err := v.Value()
			if err == nil {
				return val
			}
		}
	case driver.Valuer:
		val, err := v.Value()
		if err == nil {
			return val
		}
	}
	return nil
}

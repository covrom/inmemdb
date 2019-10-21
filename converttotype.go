package inmemdb

import (
	"fmt"
	"reflect"
)

type Converter interface {
	ConvertFrom(v interface{}) error
}

func ConvertToType(v interface{}, to reflect.Type) (interface{}, error) {
	toKind := to.Kind()
	if toKind == reflect.Ptr {
		return convertIndirect(v, to)
	}

	pto := reflect.New(to).Interface()
	if p, ok := pto.(Converter); ok {
		if err := p.ConvertFrom(v); err != nil {
			return nil, err
		}
		return reflect.ValueOf(p).Elem().Interface(), nil
	}

	if v == nil {
		return reflect.Zero(to).Interface(), nil
	}

	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Type() == to {
		return v, nil
	}

	if !reflect.TypeOf(v).ConvertibleTo(to) {
		return nil, fmt.Errorf("can't convert %v to %s", v, to)
	}

	return reflect.ValueOf(v).Convert(to).Interface(), nil
}

func convertIndirect(v interface{}, to reflect.Type) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	elemType := to.Elem()

	elemVal, err := ConvertToType(v, elemType)
	if err != nil {
		return nil, err
	}

	ptr := reflect.New(elemType)
	ptr.Elem().Set(reflect.ValueOf(elemVal))

	return ptr.Interface(), nil
}

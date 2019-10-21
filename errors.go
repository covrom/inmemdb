package inmemdb

import (
	"fmt"
	"reflect"

	"gopkg.in/go-playground/validator.v9"
)

type ErrorValidation struct {
	ID interface{}

	Type             reflect.Type
	FieldDescription FieldDescription
	Validator        string
	Param            string
	Message          string
}

type ValidationErrorDesc struct {
	ModelID   interface{} `json:"modelId,omitempty"`
	Field     string      `json:"field"`
	Namespace string      `json:"namespace"`
	Validator string      `json:"validator"`
	Param     string      `json:"param"`
	Message   string      `json:"message"`
}

func (e *ErrorValidation) ToDesc() ValidationErrorDesc {
	return ValidationErrorDesc{
		ModelID:   e.ID,
		Field:     e.FieldDescription.JsonName,
		Namespace: fmt.Sprintf("%s.%s", e.Type, e.FieldDescription.JsonName),
		Validator: e.Validator,
		Param:     e.Param,
		Message:   e.Message,
	}
}

func (e ErrorValidation) Error() string {
	msg := e.Message
	if msg == "" {
		msg = fmt.Sprintf("%s [%s]", e.Validator, e.Param)
	}

	if e.ID != nil {
		return fmt.Sprintf("%s with ID '%v': validation failed for field %s: %s", e.Type, e.ID, e.FieldDescription.JsonName, msg)
	}
	return fmt.Sprintf("%s: validation failed for field %s: %s", e.Type, e.FieldDescription.JsonName, msg)
}

type ErrorValidations []ErrorValidation

func (e ErrorValidations) Error() string {
	return "Validation errors"
}

func MakeErrorValidations(md ModelDescription, modelID interface{}, errs validator.ValidationErrors) (ErrorValidations, error) {
	ret := make(ErrorValidations, 0, len(errs))

	for _, verr := range errs {
		fd, err := md.GetColumnByFieldName(verr.Field())
		if err != nil {
			return nil, err
		}

		ret = append(ret, ErrorValidation{
			ID:               modelID,
			Type:             md.ModelType,
			FieldDescription: *fd,
			Validator:        verr.ActualTag(),
			Param:            verr.Param(),
		})
	}

	return ret, nil
}

type ErrorForbidden struct {
	Message string
}

func (e ErrorForbidden) Error() string {
	return e.Message
}

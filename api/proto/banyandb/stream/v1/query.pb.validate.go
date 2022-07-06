// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: banyandb/stream/v1/query.proto

package v1

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
	_ = sort.Sort
)

// Validate checks the field values on Element with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Element) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Element with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in ElementMultiError, or nil if none found.
func (m *Element) ValidateAll() error {
	return m.validate(true)
}

func (m *Element) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for ElementId

	if all {
		switch v := interface{}(m.GetTimestamp()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ElementValidationError{
					field:  "Timestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ElementValidationError{
					field:  "Timestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTimestamp()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ElementValidationError{
				field:  "Timestamp",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	for idx, item := range m.GetTagFamilies() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, ElementValidationError{
						field:  fmt.Sprintf("TagFamilies[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, ElementValidationError{
						field:  fmt.Sprintf("TagFamilies[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return ElementValidationError{
					field:  fmt.Sprintf("TagFamilies[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if len(errors) > 0 {
		return ElementMultiError(errors)
	}

	return nil
}

// ElementMultiError is an error wrapping multiple validation errors returned
// by Element.ValidateAll() if the designated constraints aren't met.
type ElementMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ElementMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ElementMultiError) AllErrors() []error { return m }

// ElementValidationError is the validation error returned by Element.Validate
// if the designated constraints aren't met.
type ElementValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ElementValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ElementValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ElementValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ElementValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ElementValidationError) ErrorName() string { return "ElementValidationError" }

// Error satisfies the builtin error interface
func (e ElementValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sElement.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ElementValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ElementValidationError{}

// Validate checks the field values on QueryResponse with the rules defined in
// the proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *QueryResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryResponse with the rules defined
// in the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in QueryResponseMultiError, or
// nil if none found.
func (m *QueryResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	for idx, item := range m.GetElements() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, QueryResponseValidationError{
						field:  fmt.Sprintf("Elements[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, QueryResponseValidationError{
						field:  fmt.Sprintf("Elements[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return QueryResponseValidationError{
					field:  fmt.Sprintf("Elements[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if len(errors) > 0 {
		return QueryResponseMultiError(errors)
	}

	return nil
}

// QueryResponseMultiError is an error wrapping multiple validation errors
// returned by QueryResponse.ValidateAll() if the designated constraints
// aren't met.
type QueryResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryResponseMultiError) AllErrors() []error { return m }

// QueryResponseValidationError is the validation error returned by
// QueryResponse.Validate if the designated constraints aren't met.
type QueryResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryResponseValidationError) ErrorName() string { return "QueryResponseValidationError" }

// Error satisfies the builtin error interface
func (e QueryResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryResponseValidationError{}

// Validate checks the field values on QueryRequest with the rules defined in
// the proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *QueryRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryRequest with the rules defined
// in the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in QueryRequestMultiError, or
// nil if none found.
func (m *QueryRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if m.GetMetadata() == nil {
		err := QueryRequestValidationError{
			field:  "Metadata",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetMetadata()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Metadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Metadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetMetadata()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "Metadata",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetTimeRange()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "TimeRange",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "TimeRange",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTimeRange()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "TimeRange",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for Offset

	// no validation rules for Limit

	if all {
		switch v := interface{}(m.GetOrderBy()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "OrderBy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "OrderBy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetOrderBy()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "OrderBy",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	for idx, item := range m.GetCriteria() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, QueryRequestValidationError{
						field:  fmt.Sprintf("Criteria[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, QueryRequestValidationError{
						field:  fmt.Sprintf("Criteria[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return QueryRequestValidationError{
					field:  fmt.Sprintf("Criteria[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if m.GetProjection() == nil {
		err := QueryRequestValidationError{
			field:  "Projection",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetProjection()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Projection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Projection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetProjection()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "Projection",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return QueryRequestMultiError(errors)
	}

	return nil
}

// QueryRequestMultiError is an error wrapping multiple validation errors
// returned by QueryRequest.ValidateAll() if the designated constraints aren't met.
type QueryRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryRequestMultiError) AllErrors() []error { return m }

// QueryRequestValidationError is the validation error returned by
// QueryRequest.Validate if the designated constraints aren't met.
type QueryRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryRequestValidationError) ErrorName() string { return "QueryRequestValidationError" }

// Error satisfies the builtin error interface
func (e QueryRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryRequestValidationError{}

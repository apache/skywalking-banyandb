// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: banyandb/measure/v1/query.proto

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

// Validate checks the field values on DataPoint with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *DataPoint) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on DataPoint with the rules defined in
// the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in DataPointMultiError, or nil
// if none found.
func (m *DataPoint) ValidateAll() error {
	return m.validate(true)
}

func (m *DataPoint) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetTimestamp()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, DataPointValidationError{
					field:  "Timestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, DataPointValidationError{
					field:  "Timestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTimestamp()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return DataPointValidationError{
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
					errors = append(errors, DataPointValidationError{
						field:  fmt.Sprintf("TagFamilies[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, DataPointValidationError{
						field:  fmt.Sprintf("TagFamilies[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return DataPointValidationError{
					field:  fmt.Sprintf("TagFamilies[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	for idx, item := range m.GetFields() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, DataPointValidationError{
						field:  fmt.Sprintf("Fields[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, DataPointValidationError{
						field:  fmt.Sprintf("Fields[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return DataPointValidationError{
					field:  fmt.Sprintf("Fields[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if len(errors) > 0 {
		return DataPointMultiError(errors)
	}

	return nil
}

// DataPointMultiError is an error wrapping multiple validation errors returned
// by DataPoint.ValidateAll() if the designated constraints aren't met.
type DataPointMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m DataPointMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m DataPointMultiError) AllErrors() []error { return m }

// DataPointValidationError is the validation error returned by
// DataPoint.Validate if the designated constraints aren't met.
type DataPointValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e DataPointValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e DataPointValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e DataPointValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e DataPointValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e DataPointValidationError) ErrorName() string { return "DataPointValidationError" }

// Error satisfies the builtin error interface
func (e DataPointValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sDataPoint.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = DataPointValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = DataPointValidationError{}

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

	for idx, item := range m.GetDataPoints() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, QueryResponseValidationError{
						field:  fmt.Sprintf("DataPoints[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, QueryResponseValidationError{
						field:  fmt.Sprintf("DataPoints[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return QueryResponseValidationError{
					field:  fmt.Sprintf("DataPoints[%v]", idx),
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

	if m.GetTimeRange() == nil {
		err := QueryRequestValidationError{
			field:  "TimeRange",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
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

	if all {
		switch v := interface{}(m.GetCriteria()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Criteria",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Criteria",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetCriteria()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "Criteria",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if m.GetTagProjection() == nil {
		err := QueryRequestValidationError{
			field:  "TagProjection",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetTagProjection()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "TagProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "TagProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTagProjection()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "TagProjection",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetFieldProjection()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "FieldProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "FieldProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetFieldProjection()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "FieldProjection",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetGroupBy()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "GroupBy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "GroupBy",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetGroupBy()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "GroupBy",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetAgg()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Agg",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Agg",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetAgg()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "Agg",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetTop()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Top",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequestValidationError{
					field:  "Top",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTop()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequestValidationError{
				field:  "Top",
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

// Validate checks the field values on DataPoint_Field with the rules defined
// in the proto definition for this message. If any rules are violated, the
// first error encountered is returned, or nil if there are no violations.
func (m *DataPoint_Field) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on DataPoint_Field with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// DataPoint_FieldMultiError, or nil if none found.
func (m *DataPoint_Field) ValidateAll() error {
	return m.validate(true)
}

func (m *DataPoint_Field) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Name

	if all {
		switch v := interface{}(m.GetValue()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, DataPoint_FieldValidationError{
					field:  "Value",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, DataPoint_FieldValidationError{
					field:  "Value",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetValue()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return DataPoint_FieldValidationError{
				field:  "Value",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return DataPoint_FieldMultiError(errors)
	}

	return nil
}

// DataPoint_FieldMultiError is an error wrapping multiple validation errors
// returned by DataPoint_Field.ValidateAll() if the designated constraints
// aren't met.
type DataPoint_FieldMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m DataPoint_FieldMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m DataPoint_FieldMultiError) AllErrors() []error { return m }

// DataPoint_FieldValidationError is the validation error returned by
// DataPoint_Field.Validate if the designated constraints aren't met.
type DataPoint_FieldValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e DataPoint_FieldValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e DataPoint_FieldValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e DataPoint_FieldValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e DataPoint_FieldValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e DataPoint_FieldValidationError) ErrorName() string { return "DataPoint_FieldValidationError" }

// Error satisfies the builtin error interface
func (e DataPoint_FieldValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sDataPoint_Field.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = DataPoint_FieldValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = DataPoint_FieldValidationError{}

// Validate checks the field values on QueryRequest_FieldProjection with the
// rules defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *QueryRequest_FieldProjection) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryRequest_FieldProjection with the
// rules defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// QueryRequest_FieldProjectionMultiError, or nil if none found.
func (m *QueryRequest_FieldProjection) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryRequest_FieldProjection) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if len(errors) > 0 {
		return QueryRequest_FieldProjectionMultiError(errors)
	}

	return nil
}

// QueryRequest_FieldProjectionMultiError is an error wrapping multiple
// validation errors returned by QueryRequest_FieldProjection.ValidateAll() if
// the designated constraints aren't met.
type QueryRequest_FieldProjectionMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryRequest_FieldProjectionMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryRequest_FieldProjectionMultiError) AllErrors() []error { return m }

// QueryRequest_FieldProjectionValidationError is the validation error returned
// by QueryRequest_FieldProjection.Validate if the designated constraints
// aren't met.
type QueryRequest_FieldProjectionValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryRequest_FieldProjectionValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryRequest_FieldProjectionValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryRequest_FieldProjectionValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryRequest_FieldProjectionValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryRequest_FieldProjectionValidationError) ErrorName() string {
	return "QueryRequest_FieldProjectionValidationError"
}

// Error satisfies the builtin error interface
func (e QueryRequest_FieldProjectionValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryRequest_FieldProjection.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryRequest_FieldProjectionValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryRequest_FieldProjectionValidationError{}

// Validate checks the field values on QueryRequest_GroupBy with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *QueryRequest_GroupBy) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryRequest_GroupBy with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// QueryRequest_GroupByMultiError, or nil if none found.
func (m *QueryRequest_GroupBy) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryRequest_GroupBy) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetTagProjection()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, QueryRequest_GroupByValidationError{
					field:  "TagProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, QueryRequest_GroupByValidationError{
					field:  "TagProjection",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTagProjection()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return QueryRequest_GroupByValidationError{
				field:  "TagProjection",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for FieldName

	if len(errors) > 0 {
		return QueryRequest_GroupByMultiError(errors)
	}

	return nil
}

// QueryRequest_GroupByMultiError is an error wrapping multiple validation
// errors returned by QueryRequest_GroupBy.ValidateAll() if the designated
// constraints aren't met.
type QueryRequest_GroupByMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryRequest_GroupByMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryRequest_GroupByMultiError) AllErrors() []error { return m }

// QueryRequest_GroupByValidationError is the validation error returned by
// QueryRequest_GroupBy.Validate if the designated constraints aren't met.
type QueryRequest_GroupByValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryRequest_GroupByValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryRequest_GroupByValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryRequest_GroupByValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryRequest_GroupByValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryRequest_GroupByValidationError) ErrorName() string {
	return "QueryRequest_GroupByValidationError"
}

// Error satisfies the builtin error interface
func (e QueryRequest_GroupByValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryRequest_GroupBy.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryRequest_GroupByValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryRequest_GroupByValidationError{}

// Validate checks the field values on QueryRequest_Aggregation with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *QueryRequest_Aggregation) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryRequest_Aggregation with the
// rules defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// QueryRequest_AggregationMultiError, or nil if none found.
func (m *QueryRequest_Aggregation) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryRequest_Aggregation) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Function

	// no validation rules for FieldName

	if len(errors) > 0 {
		return QueryRequest_AggregationMultiError(errors)
	}

	return nil
}

// QueryRequest_AggregationMultiError is an error wrapping multiple validation
// errors returned by QueryRequest_Aggregation.ValidateAll() if the designated
// constraints aren't met.
type QueryRequest_AggregationMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryRequest_AggregationMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryRequest_AggregationMultiError) AllErrors() []error { return m }

// QueryRequest_AggregationValidationError is the validation error returned by
// QueryRequest_Aggregation.Validate if the designated constraints aren't met.
type QueryRequest_AggregationValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryRequest_AggregationValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryRequest_AggregationValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryRequest_AggregationValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryRequest_AggregationValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryRequest_AggregationValidationError) ErrorName() string {
	return "QueryRequest_AggregationValidationError"
}

// Error satisfies the builtin error interface
func (e QueryRequest_AggregationValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryRequest_Aggregation.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryRequest_AggregationValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryRequest_AggregationValidationError{}

// Validate checks the field values on QueryRequest_Top with the rules defined
// in the proto definition for this message. If any rules are violated, the
// first error encountered is returned, or nil if there are no violations.
func (m *QueryRequest_Top) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on QueryRequest_Top with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// QueryRequest_TopMultiError, or nil if none found.
func (m *QueryRequest_Top) ValidateAll() error {
	return m.validate(true)
}

func (m *QueryRequest_Top) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Number

	// no validation rules for FieldName

	// no validation rules for FieldValueSort

	if len(errors) > 0 {
		return QueryRequest_TopMultiError(errors)
	}

	return nil
}

// QueryRequest_TopMultiError is an error wrapping multiple validation errors
// returned by QueryRequest_Top.ValidateAll() if the designated constraints
// aren't met.
type QueryRequest_TopMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m QueryRequest_TopMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m QueryRequest_TopMultiError) AllErrors() []error { return m }

// QueryRequest_TopValidationError is the validation error returned by
// QueryRequest_Top.Validate if the designated constraints aren't met.
type QueryRequest_TopValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e QueryRequest_TopValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e QueryRequest_TopValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e QueryRequest_TopValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e QueryRequest_TopValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e QueryRequest_TopValidationError) ErrorName() string { return "QueryRequest_TopValidationError" }

// Error satisfies the builtin error interface
func (e QueryRequest_TopValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sQueryRequest_Top.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = QueryRequest_TopValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = QueryRequest_TopValidationError{}

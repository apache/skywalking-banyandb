// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: banyandb/common/v1/common.proto

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

// Validate checks the field values on Metadata with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Metadata) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Metadata with the rules defined in
// the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in MetadataMultiError, or nil
// if none found.
func (m *Metadata) ValidateAll() error {
	return m.validate(true)
}

func (m *Metadata) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Group

	if utf8.RuneCountInString(m.GetName()) < 1 {
		err := MetadataValidationError{
			field:  "Name",
			reason: "value length must be at least 1 runes",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	// no validation rules for Id

	// no validation rules for CreateRevision

	// no validation rules for ModRevision

	if len(errors) > 0 {
		return MetadataMultiError(errors)
	}

	return nil
}

// MetadataMultiError is an error wrapping multiple validation errors returned
// by Metadata.ValidateAll() if the designated constraints aren't met.
type MetadataMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m MetadataMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m MetadataMultiError) AllErrors() []error { return m }

// MetadataValidationError is the validation error returned by
// Metadata.Validate if the designated constraints aren't met.
type MetadataValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e MetadataValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e MetadataValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e MetadataValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e MetadataValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e MetadataValidationError) ErrorName() string { return "MetadataValidationError" }

// Error satisfies the builtin error interface
func (e MetadataValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sMetadata.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = MetadataValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = MetadataValidationError{}

// Validate checks the field values on IntervalRule with the rules defined in
// the proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *IntervalRule) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on IntervalRule with the rules defined
// in the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in IntervalRuleMultiError, or
// nil if none found.
func (m *IntervalRule) ValidateAll() error {
	return m.validate(true)
}

func (m *IntervalRule) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if _, ok := IntervalRule_Unit_name[int32(m.GetUnit())]; !ok {
		err := IntervalRuleValidationError{
			field:  "Unit",
			reason: "value must be one of the defined enum values",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if m.GetNum() <= 0 {
		err := IntervalRuleValidationError{
			field:  "Num",
			reason: "value must be greater than 0",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return IntervalRuleMultiError(errors)
	}

	return nil
}

// IntervalRuleMultiError is an error wrapping multiple validation errors
// returned by IntervalRule.ValidateAll() if the designated constraints aren't met.
type IntervalRuleMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m IntervalRuleMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m IntervalRuleMultiError) AllErrors() []error { return m }

// IntervalRuleValidationError is the validation error returned by
// IntervalRule.Validate if the designated constraints aren't met.
type IntervalRuleValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e IntervalRuleValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e IntervalRuleValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e IntervalRuleValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e IntervalRuleValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e IntervalRuleValidationError) ErrorName() string { return "IntervalRuleValidationError" }

// Error satisfies the builtin error interface
func (e IntervalRuleValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sIntervalRule.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = IntervalRuleValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = IntervalRuleValidationError{}

// Validate checks the field values on ResourceOpts with the rules defined in
// the proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *ResourceOpts) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on ResourceOpts with the rules defined
// in the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in ResourceOptsMultiError, or
// nil if none found.
func (m *ResourceOpts) ValidateAll() error {
	return m.validate(true)
}

func (m *ResourceOpts) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if m.GetShardNum() <= 0 {
		err := ResourceOptsValidationError{
			field:  "ShardNum",
			reason: "value must be greater than 0",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if m.GetBlockInterval() == nil {
		err := ResourceOptsValidationError{
			field:  "BlockInterval",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetBlockInterval()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "BlockInterval",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "BlockInterval",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetBlockInterval()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResourceOptsValidationError{
				field:  "BlockInterval",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if m.GetSegmentInterval() == nil {
		err := ResourceOptsValidationError{
			field:  "SegmentInterval",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetSegmentInterval()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "SegmentInterval",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "SegmentInterval",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSegmentInterval()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResourceOptsValidationError{
				field:  "SegmentInterval",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if m.GetTtl() == nil {
		err := ResourceOptsValidationError{
			field:  "Ttl",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetTtl()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "Ttl",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResourceOptsValidationError{
					field:  "Ttl",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTtl()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResourceOptsValidationError{
				field:  "Ttl",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return ResourceOptsMultiError(errors)
	}

	return nil
}

// ResourceOptsMultiError is an error wrapping multiple validation errors
// returned by ResourceOpts.ValidateAll() if the designated constraints aren't met.
type ResourceOptsMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ResourceOptsMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ResourceOptsMultiError) AllErrors() []error { return m }

// ResourceOptsValidationError is the validation error returned by
// ResourceOpts.Validate if the designated constraints aren't met.
type ResourceOptsValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ResourceOptsValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ResourceOptsValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ResourceOptsValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ResourceOptsValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ResourceOptsValidationError) ErrorName() string { return "ResourceOptsValidationError" }

// Error satisfies the builtin error interface
func (e ResourceOptsValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sResourceOpts.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ResourceOptsValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ResourceOptsValidationError{}

// Validate checks the field values on Group with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Group) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Group with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in GroupMultiError, or nil if none found.
func (m *Group) ValidateAll() error {
	return m.validate(true)
}

func (m *Group) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if m.GetMetadata() == nil {
		err := GroupValidationError{
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
				errors = append(errors, GroupValidationError{
					field:  "Metadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, GroupValidationError{
					field:  "Metadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetMetadata()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return GroupValidationError{
				field:  "Metadata",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for Catalog

	if m.GetResourceOpts() == nil {
		err := GroupValidationError{
			field:  "ResourceOpts",
			reason: "value is required",
		}
		if !all {
			return err
		}
		errors = append(errors, err)
	}

	if all {
		switch v := interface{}(m.GetResourceOpts()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, GroupValidationError{
					field:  "ResourceOpts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, GroupValidationError{
					field:  "ResourceOpts",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetResourceOpts()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return GroupValidationError{
				field:  "ResourceOpts",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetUpdatedAt()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, GroupValidationError{
					field:  "UpdatedAt",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, GroupValidationError{
					field:  "UpdatedAt",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetUpdatedAt()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return GroupValidationError{
				field:  "UpdatedAt",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return GroupMultiError(errors)
	}

	return nil
}

// GroupMultiError is an error wrapping multiple validation errors returned by
// Group.ValidateAll() if the designated constraints aren't met.
type GroupMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m GroupMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m GroupMultiError) AllErrors() []error { return m }

// GroupValidationError is the validation error returned by Group.Validate if
// the designated constraints aren't met.
type GroupValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e GroupValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e GroupValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e GroupValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e GroupValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e GroupValidationError) ErrorName() string { return "GroupValidationError" }

// Error satisfies the builtin error interface
func (e GroupValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sGroup.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = GroupValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = GroupValidationError{}

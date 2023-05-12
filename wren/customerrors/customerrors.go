package customerrors

import (
	"fmt"
)

type AlreadyExistsError struct {
	Err error
}

func (a *AlreadyExistsError) Error() string {
	return a.Err.Error()
}

func NewAlreadyExistsError(item string) *AlreadyExistsError {
	return &AlreadyExistsError{Err: fmt.Errorf("%s already exists", item)}
}

func NewAlreadyExistsInError(item string, in string) *AlreadyExistsError {
	return &AlreadyExistsError{Err: fmt.Errorf("%s already exists in %s", item, in)}
}

type ComputeError struct {
	Err error
}

func (a *ComputeError) Error() string {
	return a.Err.Error()
}

func NewComputeError(statusErr error) *ComputeError {
	return &ComputeError{Err: statusErr}
}

type Violation struct {
	Kind        string
	Subject     string
	Description string
}

type FailedPreconditionError struct {
	Err        error
	Violations []*Violation
}

func (a *FailedPreconditionError) Error() string {
	return a.Err.Error()
}

func NewFailedPreconditionError(msg string) *FailedPreconditionError {
	return &FailedPreconditionError{
		Err:        fmt.Errorf("%s", msg),
		Violations: []*Violation{},
	}
}

type InternalError struct {
	Err error
}

func (a *InternalError) Error() string {
	return a.Err.Error()
}

func NewInternalError(msg string) *InternalError {
	return &InternalError{
		Err: fmt.Errorf("%s", msg),
	}
}

type InvalidArgumentError struct {
	Err error
}

func (a *InvalidArgumentError) Error() string {
	return a.Err.Error()
}

func NewInvalidArgumentError(item string) *InvalidArgumentError {
	return &InvalidArgumentError{Err: fmt.Errorf("invalid %s provided", item)}
}

func NewInvalidArgumentErrorWithCustomText(text string) *InvalidArgumentError {
	return &InvalidArgumentError{Err: fmt.Errorf(text)}
}

type NotFoundError struct {
	Err error
}

func (a *NotFoundError) Error() string {
	return a.Err.Error()
}

func NewNotFoundError(item string) *NotFoundError {
	return &NotFoundError{Err: fmt.Errorf("%s not found", item)}
}

func NewNotFoundErrorWithCustomText(text string) *NotFoundError {
	return &NotFoundError{Err: fmt.Errorf(text)}
}

type PermissionDeniedError struct {
	Err error
}

func (a *PermissionDeniedError) Error() string {
	return a.Err.Error()
}

func NewPermissionDeniedError(msg string) *PermissionDeniedError {
	return &PermissionDeniedError{Err: fmt.Errorf("%s", msg)}
}

type ResourceExhaustedError struct {
	Err error
}

func (a *ResourceExhaustedError) Error() string {
	return a.Err.Error()
}

func NewResourceExhaustedError(msg string) *ResourceExhaustedError {
	return &ResourceExhaustedError{Err: fmt.Errorf("%s", msg)}
}

type UnimplementedError struct {
	Err error
}

func (a *UnimplementedError) Error() string {
	return a.Err.Error()
}

func NewUnimplementedError(msg string) *UnimplementedError {
	return &UnimplementedError{Err: fmt.Errorf("%s", msg)}
}

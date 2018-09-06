// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import context "context"

import messaging "github.com/veritone/go-messaging-lib"
import mock "github.com/stretchr/testify/mock"

// Consumer is an autogenerated mock type for the Consumer type
type Consumer struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Consumer) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Consume provides a mock function with given fields: _a0, _a1
func (_m *Consumer) Consume(_a0 context.Context, _a1 messaging.OptionCreator) (<-chan messaging.Event, error) {
	ret := _m.Called(_a0, _a1)

	var r0 <-chan messaging.Event
	if rf, ok := ret.Get(0).(func(context.Context, messaging.OptionCreator) <-chan messaging.Event); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan messaging.Event)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, messaging.OptionCreator) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MarkOffset provides a mock function with given fields: _a0, _a1
func (_m *Consumer) MarkOffset(_a0 messaging.Event, _a1 string) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(messaging.Event, string) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

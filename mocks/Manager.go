// Code generated by mockery v1.0.0
package mocks

import context "context"
import messaging "github.com/veritone/go-messaging-lib"
import mock "github.com/stretchr/testify/mock"

// Manager is an autogenerated mock type for the Manager type
type Manager struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *Manager) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CreateTopics provides a mock function with given fields: _a0, _a1, _a2
func (_m *Manager) CreateTopics(_a0 context.Context, _a1 messaging.OptionCreator, _a2 ...string) error {
	_va := make([]interface{}, len(_a2))
	for _i := range _a2 {
		_va[_i] = _a2[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0, _a1)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, messaging.OptionCreator, ...string) error); ok {
		r0 = rf(_a0, _a1, _a2...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteTopics provides a mock function with given fields: _a0, _a1
func (_m *Manager) DeleteTopics(_a0 context.Context, _a1 ...string) error {
	_va := make([]interface{}, len(_a1))
	for _i := range _a1 {
		_va[_i] = _a1[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _a0)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, ...string) error); ok {
		r0 = rf(_a0, _a1...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ListTopics provides a mock function with given fields: _a0
func (_m *Manager) ListTopics(_a0 context.Context) (interface{}, error) {
	ret := _m.Called(_a0)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(context.Context) interface{}); ok {
		r0 = rf(_a0)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

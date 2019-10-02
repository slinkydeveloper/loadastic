package test

import "fmt"

type mockRequest interface{}
type mockResponse interface{}

type mockSender struct {
	count    int
	mustFail bool
}

func (m *mockSender) Send(request mockRequest) (mockResponse, error) {
	m.count++
	if m.mustFail {
		return nil, fmt.Errorf("bla")
	} else {
		return nil, nil
	}
}

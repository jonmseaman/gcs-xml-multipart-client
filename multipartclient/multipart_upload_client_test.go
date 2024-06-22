package multipartclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var (
	errMock = errors.New("mock error for testing")

	ignoreXmlName = cmpopts.IgnoreFields(InitiateMultipartUploadResult{}, "XMLName")
	strCompareOpt = cmp.Transformer("fix_whitespace", func(in string) string {
		out := strings.ReplaceAll(in, "\r", "")
		return out
	})
)

func httpReqToStr(t *testing.T, req *http.Request) string {
	t.Helper()

	reqBytes, err := httputil.DumpRequest(req, true)
	if err != nil {
		t.Fatal(err)
	}

	return string(reqBytes)
}

// reqRecordingTransport intercepts a request and converts it to a string for testing.
type reqRecordingTransport struct {
	t   *testing.T
	req string
}

func newReqRecordingTransport(t *testing.T) *reqRecordingTransport {
	return &reqRecordingTransport{
		t: t,
	}
}

func (rrt *reqRecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rrt.t.Helper()
	rrt.req = httpReqToStr(rrt.t, req)
	return nil, errMock
}

type mockTransport struct {
	t *testing.T
	// Response to responsd with when RoundTrip is called.
	resp *http.Response
	// Error to response with when RoundTrip is called.
	err error
}

func newMockTransport(t *testing.T, resp *http.Response, err error) *mockTransport {
	return &mockTransport{
		t:    t,
		resp: resp,
		err:  err,
	}
}

func (mt *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return mt.resp, mt.err
}

func TestInititateMultipartUploadRequests(t *testing.T) {
	tests := []struct {
		req         *InitiateMultipartUploadRequest
		wantHttpReq string
	}{
		{
			req: &InitiateMultipartUploadRequest{
				Bucket: "bucket1",
				Key:    "file1.txt",
			},
			wantHttpReq: "POST /bucket1/file1.txt?uploads HTTP/1.1\n" +
				"Host: storage.googleapis.com\n\n",
		},
		{
			req: &InitiateMultipartUploadRequest{
				Bucket: "bucket1",
				Key:    "some/file/with/a/path/file1.txt",
			},
			wantHttpReq: "POST /bucket1/some/file/with/a/path/file1.txt?uploads HTTP/1.1\n" +
				"Host: storage.googleapis.com\n\n",
		},
	}

	for _, tc := range tests {
		trans := newReqRecordingTransport(t)
		hc := &http.Client{
			Transport: trans,
		}
		mpuc := New(hc)
		ctx := context.Background()
		_, err := mpuc.InitiateMultipartUpload(ctx, tc.req)
		if !strings.Contains(err.Error(), errMock.Error()) {
			t.Fatal(err)
		}
		if diff := cmp.Diff(tc.wantHttpReq, trans.req, strCompareOpt); diff != "" {
			t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
		}
	}
}

func strToReadCloser(str string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(str))
}

func TestInititateMultipartUploadResponse(t *testing.T) {
	req := &InitiateMultipartUploadRequest{
		Bucket: "test-bucket",
		Key:    "obj.txt",
	}
	tests := []struct {
		resp       *http.Response
		wantParsed *InitiateMultipartUploadResult
	}{
		{
			resp: &http.Response{
				Status:        "OK",
				StatusCode:    http.StatusOK,
				ContentLength: 280,
				Header: http.Header{
					"Content-Type": []string{"application/xml"},
					"Date":         []string{"Wed, 24 Mar 2021 18:11:53 GMT"},
				},
				Body: strToReadCloser(`<?xml version="1.0" encoding="UTF-8"?>
					<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					  <Bucket>travel-maps</Bucket>
					  <Key>paris.jpg</Key>
					  <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
					</InitiateMultipartUploadResult>`),
			},
			wantParsed: &InitiateMultipartUploadResult{
				Bucket:   "travel-maps",
				Key:      "paris.jpg",
				UploadID: "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA",
			},
		},
	}

	for _, tc := range tests {
		trans := newMockTransport(t, tc.resp, nil)
		hc := &http.Client{
			Transport: trans,
		}
		mpuc := New(hc)
		ctx := context.Background()
		resp, err := mpuc.InitiateMultipartUpload(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(tc.wantParsed, resp, ignoreXmlName); diff != "" {
			t.Errorf("unexpected diff: (-want, +got):\n%s", diff)
		}
	}
}

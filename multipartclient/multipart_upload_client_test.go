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

type mockTransport struct {
	t               *testing.T
	recordedHttpReq string
	// Response to responsd with when RoundTrip is called.
	respondWithHttp *http.Response
	// Error to response with when RoundTrip is called.
	respondWithErr error
}

func (mt *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	mt.recordedHttpReq = httpReqToStr(mt.t, req)
	return mt.respondWithHttp, mt.respondWithErr
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
		trans := &mockTransport{
			t:               t,
			respondWithHttp: nil,
			respondWithErr:  errMock,
		}
		hc := &http.Client{
			Transport: trans,
		}
		mpuc := New(hc)
		ctx := context.Background()
		_, err := mpuc.InitiateMultipartUpload(ctx, tc.req)
		if !strings.Contains(err.Error(), errMock.Error()) {
			t.Fatal(err)
		}
		if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
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
		trans := &mockTransport{
			t:               t,
			respondWithHttp: tc.resp,
			respondWithErr:  nil,
		}
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

func compareErrorValues() cmp.Option {
	return cmp.Comparer(func(x error, y error) bool {
		if x == y {
			return true
		}
		if x != nil && y != nil {
			return x.Error() == y.Error()
		}

		return false
	})
}

func TestAbortMultipartUploads(t *testing.T) {
	success := &http.Response{
		Status:     http.StatusText(http.StatusNoContent),
		StatusCode: http.StatusNoContent,
	}
	notFound := &http.Response{
		Status:     http.StatusText(http.StatusNotFound),
		StatusCode: http.StatusNotFound,
	}

	tests := []struct {
		name        string
		req         *AbortMultipartUploadRequest
		wantHttpReq string
		httpResp    *http.Response
		wantResult  error
	}{
		{
			name: "Abort with a success",
			req: &AbortMultipartUploadRequest{
				Bucket:   "bucket1",
				Key:      "file1.txt",
				UploadID: "my-upload-id",
			},
			wantHttpReq: "DELETE /bucket1/file1.txt?uploadId=my-upload-id HTTP/1.1\n" +
				"Host: storage.googleapis.com\n\n",
			httpResp:   success,
			wantResult: nil,
		},
		{
			name: "Abort with a not found error",
			req: &AbortMultipartUploadRequest{
				Bucket:   "bucket1",
				Key:      "some/file/with/a/path/file1.txt",
				UploadID: "my-upload-id",
			},
			wantHttpReq: "DELETE /bucket1/some/file/with/a/path/file1.txt?uploadId=my-upload-id HTTP/1.1\n" +
				"Host: storage.googleapis.com\n\n",
			httpResp:   notFound,
			wantResult: errors.New("Not Found"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trans := &mockTransport{
				t:               t,
				respondWithHttp: tc.httpResp,
				respondWithErr:  nil,
			}
			hc := &http.Client{
				Transport: trans,
			}
			mpuc := New(hc)
			ctx := context.Background()
			err := mpuc.AbortMultipartUpload(ctx, tc.req)

			// Verify request.
			if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
				t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
			}

			// Verify response.
			if diff := cmp.Diff(tc.wantResult, err, compareErrorValues()); diff != "" {
				t.Errorf("unexpected diff for abort result: (-want, +got):\n%s", diff)
			}

		})
	}
}

func TestListMultipartUploads(t *testing.T) {

	listHttpResp := &http.Response{
		Status:     http.StatusText(http.StatusOK),
		StatusCode: http.StatusOK,
		Body: strToReadCloser(`<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>travel-maps</Bucket>
  <KeyMarker></KeyMarker>
  <UploadIdMarker></UploadIdMarker>
  <NextKeyMarker>cannes.jpeg</NextKeyMarker>
  <NextUploadIdMarker>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</NextUploadIdMarker>
  <MaxUploads>2</MaxUploads>
  <IsTruncated>true</IsTruncated>
  <Upload>
    <Key>paris.jpeg</Key>
    <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2021-11-10T20:48:33.000Z</Initiated>
  </Upload>
  <Upload>
    <Key>tokyo.jpeg</Key>
    <UploadId>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</UploadId>
    <StorageClass>STANDARD</StorageClass>
    <Initiated>2021-11-10T20:49:33.000Z</Initiated>
  </Upload>
</ListMultipartUploadsResult>`),
	}
	tests := []struct {
		name          string
		req           *ListMultipartUploadsRequest
		wantHttpReq   string
		httpResp      *http.Response
		wantResult    *ListMultipartUploadsResult
		wantResultErr error
	}{
		{
			name: "List with a success",
			req: &ListMultipartUploadsRequest{
				Bucket: "bucket1",
			},
			wantHttpReq: "GET /bucket1/?uploads HTTP/1.1\n" +
				"Host: storage.googleapis.com\n\n",
			httpResp: listHttpResp,
			wantResult: &ListMultipartUploadsResult{
				Uploads: []ListUpload{
					{
						UploadID: "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA",
					},
					{
						UploadID: "YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ",
					},
				},
			},
			wantResultErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trans := &mockTransport{
				t:               t,
				respondWithHttp: tc.httpResp,
				respondWithErr:  nil,
			}
			hc := &http.Client{
				Transport: trans,
			}
			mpuc := New(hc)
			ctx := context.Background()
			listResult, err := mpuc.ListMultipartUploads(ctx, tc.req)

			// Verify request.
			if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
				t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
			}

			// Verify response.
			opts := []cmp.Option{
				cmpopts.IgnoreFields(ListMultipartUploadsResult{}, "XMLName"),
				cmpopts.IgnoreFields(ListUpload{}, "XMLName"),
			}
			if diff := cmp.Diff(tc.wantResult, listResult, opts...); diff != "" {
				t.Errorf("unexpected diff for list result: (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantResultErr, err, compareErrorValues()); diff != "" {
				t.Errorf("unexpected diff for error: (-want, +got):\n%s", diff)
			}

		})
	}
}

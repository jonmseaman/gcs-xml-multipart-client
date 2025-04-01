package multipartclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"testing"
	"time"

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

func newFake(hc *http.Client) *MultipartClient {
	c := New(hc)
	c.now = func() time.Time {
		return time.Unix(0, 0)
	}

	return c
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
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n" +
				"\n",
		},
		{
			req: &InitiateMultipartUploadRequest{
				Bucket: "bucket1",
				Key:    "some/file/with/a/path/file1.txt",
			},
			wantHttpReq: "POST /bucket1/some/file/with/a/path/file1.txt?uploads HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n" +
				"\n",
		},
		{
			req: &InitiateMultipartUploadRequest{
				Bucket: "bucket1",
				Key:    "some/file/with/a/path/file1.txt",
				CustomMetadata: map[string]string{
					"mtime": "Saturday",
					"ctime": "Friday",
				},
			},
			wantHttpReq: "POST /bucket1/some/file/with/a/path/file1.txt?uploads HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n" +
				"X-Goog-Meta-Ctime: Friday\n" +
				"X-Goog-Meta-Mtime: Saturday\n" +
				"\n",
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
		mpuc := newFake(hc)
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

func toBody(str string) io.ReadCloser {
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
				Body: toBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
					"  <Bucket>travel-maps</Bucket>\n" +
					"  <Key>paris.jpg</Key>\n" +
					"  <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>\n" +
					"</InitiateMultipartUploadResult>"),
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
		mpuc := newFake(hc)
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

func TestUploadObjectPart(t *testing.T) {
	tests := []struct {
		name          string
		req           *UploadObjectPartRequest
		wantHttpReq   string
		httpResp      *http.Response
		wantResultErr error
	}{
		{
			name: "Upload part success",
			req: &UploadObjectPartRequest{
				Bucket:     "bucket1",
				Key:        "object.txt",
				PartNumber: 2,
				UploadID:   "my-upload-id",
				Body:       toBody("part contents"),
			},
			wantHttpReq: "PUT /bucket1/object.txt?partNumber=2&uploadId=my-upload-id HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n" +
				"part contents",
			httpResp: &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			},
			wantResultErr: nil,
		},
		{
			name: "Upload part 404 error",
			req: &UploadObjectPartRequest{
				Bucket:     "bucket1",
				Key:        "object.txt",
				PartNumber: 2,
				UploadID:   "my-upload-id",
				Body:       toBody("part contents"),
			},
			wantHttpReq: "PUT /bucket1/object.txt?partNumber=2&uploadId=my-upload-id HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n" +
				"part contents",
			httpResp: &http.Response{
				Status:     http.StatusText(http.StatusNotFound),
				StatusCode: http.StatusNotFound,
				Body:       toBody("Bucket not found."),
			},
			wantResultErr: errors.New("Bucket not found."),
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
			mpuc := newFake(hc)
			ctx := context.Background()
			err := mpuc.UploadObjectPart(ctx, tc.req)

			// Verify request.
			if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
				t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
			}

			// Verify response.
			if diff := cmp.Diff(tc.wantResultErr, err, compareErrorValues()); diff != "" {
				t.Errorf("unexpected diff for error: (-want, +got):\n%s", diff)
			}

		})
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	tests := []struct {
		name          string
		req           *CompleteMultipartUploadRequest
		wantHttpReq   string
		httpResp      *http.Response
		wantResult    *CompleteMultipartUploadResult
		wantResultErr error
	}{
		{

			name: "Successful request",
			req: &CompleteMultipartUploadRequest{
				Bucket:   "test-bucket",
				Key:      "object.txt",
				UploadID: "test-upload-id",
				Body: CompleteMultipartUploadBody{
					Parts: []CompletePart{
						{
							PartNumber: 1,
							Etag:       "etagpart1",
						},
						{
							PartNumber: 2,
							Etag:       "etagpart2",
						},
					},
				},
			},

			wantHttpReq: "POST /test-bucket/object.txt?uploadId=test-upload-id HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"ContentLength: 206\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n" +
				"\n" +
				"<CompleteMultipartUpload>\n" +
				"  <Part>\n" +
				"    <PartNumber>1</PartNumber>\n" +
				"    <ETag>etagpart1</ETag>\n" +
				"  </Part>\n" +
				"  <Part>\n" +
				"    <PartNumber>2</PartNumber>\n" +
				"    <ETag>etagpart2</ETag>\n" +
				"  </Part>\n" +
				"</CompleteMultipartUpload>",
			httpResp: &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body: toBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
					"  <Location>https://storage.googleapis.com/test-bucket/object.txt</Location>\n" +
					"  <Bucket>test-bucket</Bucket>\n" +
					"  <Key>object.txt</Key>\n" +
					"  <ETag>etag</ETag>\n" +
					"</CompleteMultipartUploadResult>"),
			},
			wantResult: &CompleteMultipartUploadResult{
				Location: "https://storage.googleapis.com/test-bucket/object.txt",
				Bucket:   "test-bucket",
				Key:      "object.txt",
				Etag:     "etag",
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
			mpuc := newFake(hc)
			ctx := context.Background()
			result, err := mpuc.CompleteMultipartUpload(ctx, tc.req)

			// Verify request.
			if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
				t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
			}

			// Verify response.
			opts := []cmp.Option{
				cmpopts.IgnoreFields(CompleteMultipartUploadResult{}, "XMLName"),
			}
			if diff := cmp.Diff(tc.wantResult, result, opts...); diff != "" {
				t.Errorf("unexpected diff for result: (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantResultErr, err, compareErrorValues()); diff != "" {
				t.Errorf("unexpected diff for error: (-want, +got):\n%s", diff)
			}

		})
	}
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
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n",
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
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n",
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
			mpuc := newFake(hc)
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
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n",
			httpResp: &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body: toBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
					"  <Bucket>travel-maps</Bucket>\n" +
					"  <KeyMarker></KeyMarker>\n" +
					"  <UploadIdMarker></UploadIdMarker>\n" +
					"  <NextKeyMarker>cannes.jpeg</NextKeyMarker>\n" +
					"  <NextUploadIdMarker>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</NextUploadIdMarker>\n" +
					"  <MaxUploads>2</MaxUploads>\n" +
					"  <IsTruncated>true</IsTruncated>\n" +
					"  <Upload>\n" +
					"    <Key>paris.jpeg</Key>\n" +
					"    <UploadId>VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>\n" +
					"    <StorageClass>STANDARD</StorageClass>\n" +
					"    <Initiated>2021-11-10T20:48:33.000Z</Initiated>\n" +
					"  </Upload>\n" +
					"  <Upload>\n" +
					"    <Key>tokyo.jpeg</Key>\n" +
					"    <UploadId>YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ</UploadId>\n" +
					"    <StorageClass>STANDARD</StorageClass>\n" +
					"    <Initiated>2021-11-10T20:49:33.000Z</Initiated>\n" +
					"  </Upload>\n" +
					"</ListMultipartUploadsResult>\n"),
			},
			wantResult: &ListMultipartUploadsResult{
				Bucket:             "travel-maps",
				NextKeyMarker:      "cannes.jpeg",
				NextUploadIdMarker: "YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ",
				MaxUploads:         2,
				IsTruncated:        true,
				Uploads: []ListUpload{
					{
						Key:          "paris.jpeg",
						UploadID:     "VXBsb2FkIElEIGZvciBlbHZpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA",
						StorageClass: "STANDARD",
						Initiated:    time.Date(2021, 11, 10, 20, 48, 33, 0, time.UTC),
					},
					{
						Key:          "tokyo.jpeg",
						UploadID:     "YW55IGlkZWEgd2h5IGVsdmluZydzIHVwbG9hZCBmYWlsZWQ",
						StorageClass: "STANDARD",
						Initiated:    time.Date(2021, 11, 10, 20, 49, 33, 0, time.UTC),
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
			mpuc := newFake(hc)
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

func TestListObjectParts(t *testing.T) {
	tests := []struct {
		name          string
		req           *ListObjectPartsRequest
		wantHttpReq   string
		httpResp      *http.Response
		wantResult    *ListObjectPartsResult
		wantResultErr error
	}{
		{

			name: "Successful request",
			req: &ListObjectPartsRequest{
				Bucket:           "test-bucket",
				Key:              "object.txt",
				UploadID:         "test-upload-id",
				MaxParts:         2,
				PartNumberMarker: 1,
			},

			wantHttpReq: "GET /test-bucket/object.txt?uploadId=test-upload-id&max-parts=2&part-number-marker=1 HTTP/1.1\n" +
				"Host: storage.googleapis.com\n" +
				"Date: Thu, 01 Jan 1970 00:00:00 UTC\n\n",
			httpResp: &http.Response{
				Status:     http.StatusText(http.StatusOK),
				StatusCode: http.StatusOK,
				Body: toBody("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					"<ListPartsResult>\n" +
					"  <Bucket>test-bucket</Bucket>\n" +
					"  <Key>object.txt</Key>\n" +
					"  <UploadId>test-upload-id</UploadId>\n" +
					"  <StorageClass>STANDARD</StorageClass>\n" +
					"  <PartNumberMarker>1</PartNumberMarker>\n" +
					"  <NextPartNumberMarker>2</NextPartNumberMarker>\n" +
					"  <MaxParts>2</MaxParts>\n" +
					"  <IsTruncated>true</IsTruncated>\n" +
					"  <Part>\n" +
					"    <PartNumber>1</PartNumber>\n" +
					"    <LastModified>2021-11-10T20:48:33.000Z</LastModified>\n" +
					"    <ETag>etagpart1</ETag>\n" +
					"    <Size>1024</Size>\n" +
					"  </Part>\n" +
					"  <Part>\n" +
					"    <PartNumber>2</PartNumber>\n" +
					"    <LastModified>2021-11-10T20:48:33.000Z</LastModified>\n" +
					"    <ETag>etagpart2</ETag>\n" +
					"    <Size>1024</Size>\n" +
					"  </Part>\n" +
					"</ListPartsResult>"),
			},
			wantResult: &ListObjectPartsResult{
				Parts: []ListObjectPartsResultPart{
					{
						PartNumber: 1,
						Etag:       "etagpart1",
					},
					{
						PartNumber: 2,
						Etag:       "etagpart2",
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
			mpuc := newFake(hc)
			ctx := context.Background()
			result, err := mpuc.ListObjectParts(ctx, tc.req)

			// Verify request.
			if diff := cmp.Diff(tc.wantHttpReq, trans.recordedHttpReq, strCompareOpt); diff != "" {
				t.Errorf("unexpected diff for http request: (-want, +got):\n%s", diff)
			}

			// Verify response.
			opts := []cmp.Option{
				cmpopts.IgnoreFields(ListObjectPartsResultPart{}, "XMLName"),
			}
			if diff := cmp.Diff(tc.wantResult, result, opts...); diff != "" {
				t.Errorf("unexpected diff for result: (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantResultErr, err, compareErrorValues()); diff != "" {
				t.Errorf("unexpected diff for error: (-want, +got):\n%s", diff)
			}

		})
	}
}

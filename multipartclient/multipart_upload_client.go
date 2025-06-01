package multipartclient

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
)

// Client for using GCS XML Multipart API:
// https://cloud.google.com/storage/docs/multipart-uploads
type MultipartClient struct {
	hc  *http.Client
	now func() time.Time
}

// Create a multipart client that uses the specified http.Client.
func New(hc *http.Client) *MultipartClient {
	return &MultipartClient{
		hc:  hc,
		now: time.Now,
	}
}

func checkResponse(resp *http.Response) error {
	if 200 <= resp.StatusCode && resp.StatusCode < 300 {
		return nil
	}
	// Default to a basic message if there is no body.
	errStr := resp.Status
	if resp.Body != nil {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("%w (failed to read response body); %s", readErr, errStr)
		}
		if bodyStr := string(body); bodyStr != "" {
			errStr = bodyStr
		}
	}

	return errors.New(errStr)
}

// Initiate Multipart Upload Request
// https://cloud.google.com/storage/docs/xml-api/post-object-multipart
type InitiateMultipartUploadRequest struct {
	Bucket string
	Key    string

	// Custom metadata
	CustomMetadata map[string]string
}

// Initiate Multipart Upload Response
// https://cloud.google.com/storage/docs/xml-api/post-object-multipart
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// InitiateMultipartUpload calls the XML Multipart API to Inititate a Multipart Upload.
func (mpuc *MultipartClient) InitiateMultipartUpload(ctx context.Context, req *InitiateMultipartUploadRequest) (*InitiateMultipartUploadResult, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploads", req.Bucket, req.Key)
	httpReq, err := http.NewRequest("POST", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	// Required headers per documentation
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))
	httpReq.Header.Set("Content-Length", "0") // Required: 0 for initiate request

	// Add custom metadata:
	for key, value := range req.CustomMetadata {
		httpReq.Header.Add(fmt.Sprintf("x-goog-meta-%s", key), value)
	}

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	result := &InitiateMultipartUploadResult{}
	xml := xml.NewDecoder(resp.Body)
	if err := xml.Decode(result); err != nil {
		respStrBuilder := &strings.Builder{}
		// strings.Builder.Write does not return errors.
		resp.Write(respStrBuilder)
		return nil, fmt.Errorf("failed to parse XML body from HTTP response: %v. Response: %v", err, respStrBuilder.String())
	}
	return result, nil
}

type UploadObjectPartRequest struct {
	// Query string parameters
	Bucket     string
	Key        string
	PartNumber int
	UploadID   string

	// Headers
	CRC32C        string
	MD5           string
	ContentLength int64 // Length of the part data.

	// Object body part contents.
	Body io.ReadCloser
}

type UploadObjectPartResult struct {
	ETag   string
	CRC32C string
	MD5    string
}

// Upload an object part request.
// https://cloud.google.com/storage/docs/xml-api/put-object-multipart
func (mpuc *MultipartClient) UploadObjectPart(ctx context.Context, req *UploadObjectPartRequest) (*UploadObjectPartResult, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?partNumber=%v&uploadId=%s", req.Bucket, req.Key, req.PartNumber, req.UploadID)
	httpReq, err := http.NewRequest(http.MethodPut, url, req.Body)
	if err != nil {
		return nil, err
	}
	// Date is a required header.
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))

	// Set Content-Length if provided.
	if req.ContentLength > 0 {
		httpReq.Header.Set("Content-Length", fmt.Sprintf("%d", req.ContentLength))
	}

	if req.MD5 != "" {
		httpReq.Header["Content-MD5"] = []string{req.MD5}
		httpReq.Header.Add("X-Goog-Hash", fmt.Sprintf("md5=%s", req.MD5))
	}
	if req.CRC32C != "" {
		httpReq.Header.Add("X-Goog-Hash", fmt.Sprintf("crc32c=%s", req.CRC32C))
	}

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	result := &UploadObjectPartResult{
		ETag: resp.Header.Get("ETag"),
	}
	if resp.Header.Get("X-Goog-Hash") != "" {
		for _, val := range resp.Header["X-Goog-Hash"] {
			if strings.HasPrefix(val, "crc32c=") {
				result.CRC32C = strings.TrimPrefix(val, "crc32c=")
			}
			if strings.HasPrefix(val, "md5=") {
				result.MD5 = strings.TrimPrefix(val, "md5=")
			}
		}
	}
	return result, nil
}

type CompletePart struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int      `xml:"PartNumber"`
	Etag       string   `xml:"ETag"`
}

type CompleteMultipartUploadBody struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart
}

// TODO: Add header support.
type CompleteMultipartUploadRequest struct {
	Bucket   string
	Key      string
	UploadID string
	Body     CompleteMultipartUploadBody
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	Etag     string   `xml:"ETag"`
}

type CompleteMultipartUploadResponse struct {
	Result CompleteMultipartUploadResult
	Hash   string
}

// Complete a multipart upload.
// https://cloud.google.com/storage/docs/xml-api/post-object-complete
func (mpuc *MultipartClient) CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResult, error) {
	xmlBody := &strings.Builder{}
	encoder := xml.NewEncoder(xmlBody)
	encoder.Indent("", "  ")
	err := encoder.Encode(req.Body)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID)
	strBody := xmlBody.String()
	httpBody := io.NopCloser(strings.NewReader(strBody))
	httpReq, err := http.NewRequest(http.MethodPost, url, httpBody)
	if err != nil {
		return nil, err
	}

	// Date is a required header.
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))
	httpReq.Header["ContentLength"] = []string{fmt.Sprint(len(strBody))}

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	result := &CompleteMultipartUploadResult{}
	err = xml.Unmarshal(bodyBytes, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

type AbortMultipartUploadRequest struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

// Abort multipart upload.
// https://cloud.google.com/storage/docs/xml-api/delete-multipart
func (mpuc *MultipartClient) AbortMultipartUpload(ctx context.Context, req *AbortMultipartUploadRequest) error {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID)
	httpReq, err := http.NewRequest("DELETE", url, http.NoBody)
	if err != nil {
		return err
	}

	// Date is a required header.
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))
	httpReq.Header.Set("Content-Length", "0")

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return err
	}
	if err := checkResponse(resp); err != nil {
		return err
	}

	return nil
}

type ListMultipartUploadsRequest struct {
	Bucket string

	// Query string parameters for pagination and filtering
	KeyMarker      string
	MaxUploads     int
	Prefix         string
	UploadIdMarker string
}

// TODO: Support headers
type ListUpload struct {
	XMLName      xml.Name  `xml:"Upload"`
	Key          string    `xml:"Key"`
	UploadID     string    `xml:"UploadId"`
	StorageClass string    `xml:"StorageClass"`
	Initiated    time.Time `xml:"Initiated"`
}

// https://cloud.google.com/storage/docs/xml-api/get-bucket-uploads
type ListMultipartUploadsResult struct {
	XMLName            xml.Name     `xml:"ListMultipartUploadsResult"`
	Bucket             string       `xml:"Bucket"`
	KeyMarker          string       `xml:"KeyMarker"`
	UploadIdMarker     string       `xml:"UploadIdMarker"`
	NextKeyMarker      string       `xml:"NextKeyMarker"`
	NextUploadIdMarker string       `xml:"NextUploadIdMarker"`
	MaxUploads         int          `xml:"MaxUploads"`
	IsTruncated        bool         `xml:"IsTruncated"`
	Uploads            []ListUpload `xml:"Upload"`
}

// List Multipart Uploads
// https://cloud.google.com/storage/docs/xml-api/get-bucket-uploads
func (mpuc *MultipartClient) ListMultipartUploads(ctx context.Context, req *ListMultipartUploadsRequest) (*ListMultipartUploadsResult, error) {
	// Build URL with query parameters
	baseURL := fmt.Sprintf("https://storage.googleapis.com/%s/?uploads", req.Bucket)

	// Build query parameters
	params := url.Values{}

	if req.KeyMarker != "" {
		params.Add("key-marker", req.KeyMarker)
	}
	if req.MaxUploads > 0 {
		params.Add("max-uploads", fmt.Sprintf("%d", req.MaxUploads))
	}
	if req.Prefix != "" {
		params.Add("prefix", req.Prefix)
	}
	if req.UploadIdMarker != "" {
		params.Add("upload-id-marker", req.UploadIdMarker)
	}

	// Construct final URL
	finalURL := baseURL
	if len(params) > 0 {
		finalURL = baseURL + "&" + params.Encode()
	}

	httpReq, err := http.NewRequest(http.MethodGet, finalURL, http.NoBody)
	if err != nil {
		return nil, err
	}

	// Date is a required header.
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))
	httpReq.Header.Set("Content-Length", "0")

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	result := &ListMultipartUploadsResult{}
	xml := xml.NewDecoder(resp.Body)
	if err := xml.Decode(result); err != nil {
		respStrBuilder := &strings.Builder{}
		// strings.Builder.Write does not return errors.
		_ = resp.Write(respStrBuilder)
		return nil, fmt.Errorf("failed to parse XML body from HTTP response: %v. Response: %v", err, respStrBuilder.String())
	}
	return result, nil
}

type ListObjectPartsRequest struct {
	Bucket           string
	Key              string
	UploadID         string
	MaxParts         int
	PartNumberMarker int
}

type ListObjectPartsResultPart struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int      `xml:"PartNumber"`
	Etag       string   `xml:"ETag"`
}

type ListObjectPartsResult struct {
	Parts []ListObjectPartsResultPart `xml:"Part"`
}

// List Object Parts
// https://cloud.google.com/storage/docs/xml-api/get-object-multipart
func (mpuc *MultipartClient) ListObjectParts(ctx context.Context, req *ListObjectPartsRequest) (*ListObjectPartsResult, error) {
	url := strings.Builder{}
	url.WriteString(fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID))
	if req.MaxParts > 0 {
		url.WriteString(fmt.Sprintf("&max-parts=%d", req.MaxParts))
	}
	if req.PartNumberMarker > 0 {
		url.WriteString(fmt.Sprintf("&part-number-marker=%d", req.PartNumberMarker))
	}
	httpReq, err := http.NewRequest(http.MethodGet, url.String(), http.NoBody)
	if err != nil {
		return nil, err
	}

	// Date is a required header.
	httpReq.Header.Set("Date", mpuc.now().UTC().Format(time.RFC1123))

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	result := &ListObjectPartsResult{}
	xml := xml.NewDecoder(resp.Body)
	if err := xml.Decode(result); err != nil {
		respStrBuilder := &strings.Builder{}
		// strings.Builder.Write does not return errors.
		_ = resp.Write(respStrBuilder)
		return nil, fmt.Errorf("failed to parse XML body from HTTP response: %v. Response: %v", err, respStrBuilder.String())
	}
	return result, nil
}

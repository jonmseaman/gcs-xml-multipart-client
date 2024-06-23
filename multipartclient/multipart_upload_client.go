package multipartclient

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/api/googleapi"
)

type multipartClient struct {
	hc *http.Client
}

func New(hc *http.Client) *multipartClient {
	return &multipartClient{
		hc: hc,
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

type InitiateMultipartUploadRequest struct {
	Bucket string
	Key    string
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// InitiateMultipartUpload calls the XML Multipart API to Inititate a Multipart Upload.
func (mpuc *multipartClient) InitiateMultipartUpload(ctx context.Context, req *InitiateMultipartUploadRequest) (*InitiateMultipartUploadResult, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploads", req.Bucket, req.Key)
	httpReq, err := http.NewRequest("POST", url, http.NoBody)
	if err != nil {
		return nil, err
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
	Bucket     string
	Key        string
	PartNumber int
	UploadID   string
	Body       io.ReadCloser
}

func (mpuc *multipartClient) UploadObjectPart(ctx context.Context, req *UploadObjectPartRequest) error {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?partNumber=%v&uploadId=%s", req.Bucket, req.Key, req.PartNumber, req.UploadID)
	httpReq, err := http.NewRequest(http.MethodPut, url, req.Body)
	if err != nil {
		return err
	}

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

type CompletePart struct {
	PartNumber int `xml:"PartNumber"`
}

type CompleteMultipartUploadBody struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart
}

type CompleteMultipartUploadRequest struct {
	Bucket   string
	Key      string
	UploadID string
	Body     CompleteMultipartUploadBody
}

type CompleteMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
}

func (mpuc *multipartClient) CompleteMultipartUpload(ctx context.Context, req *CompleteMultipartUploadRequest) (*CompleteMultipartUploadResult, error) {
	xmlBody := &strings.Builder{}
	encoder := xml.NewEncoder(xmlBody)
	encoder.Indent("", "  ")
	err := encoder.Encode(req.Body)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID)
	httpBody := io.NopCloser(strings.NewReader(xmlBody.String()))
	httpReq, err := http.NewRequest(http.MethodPost, url, httpBody)
	if err != nil {
		return nil, err
	}

	resp, err := mpuc.hc.Do(httpReq.WithContext(ctx))
	defer googleapi.CloseBody(resp)
	if err != nil {
		return nil, err
	}
	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	result := &CompleteMultipartUploadResult{}
	return result, nil
}

type AbortMultipartUploadRequest struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

func (mpuc *multipartClient) AbortMultipartUpload(ctx context.Context, req *AbortMultipartUploadRequest) error {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID)
	httpReq, err := http.NewRequest("DELETE", url, http.NoBody)
	if err != nil {
		return err
	}

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
}

type ListUpload struct {
	XMLName  xml.Name `xml:"Upload"`
	UploadID string   `xml:"UploadId"`
}
type ListMultipartUploadsResult struct {
	XMLName xml.Name     `xml:"ListMultipartUploadsResult"`
	Uploads []ListUpload `xml:"Upload"`
}

func (mpuc *multipartClient) ListMultipartUploads(ctx context.Context, req *ListMultipartUploadsRequest) (*ListMultipartUploadsResult, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/?uploads", req.Bucket)
	httpReq, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

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
	Bucket   string
	Key      string
	UploadID string
}

type ListObjectPartsResult struct {
	Parts []CompletePart
}

func (mpuc *multipartClient) ListObjectParts(ctx context.Context, req *ListObjectPartsRequest) (*ListObjectPartsResult, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s?uploadId=%s", req.Bucket, req.Key, req.UploadID)
	httpReq, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

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

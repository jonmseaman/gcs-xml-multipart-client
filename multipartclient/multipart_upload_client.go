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

type multipartClient struct {
	hc *http.Client
}

func New(hc *http.Client) *multipartClient {
	return &multipartClient{
		hc: hc,
	}
}

func checkResponse(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
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

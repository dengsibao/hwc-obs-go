package client

import (
	"fmt"
	"huaweicloud-obs-go/obs"
	"io"
)

type ObsClientAuth struct {
	AccessKey string
	SecretKey string
	Endpoint string
}

func (auth *ObsClientAuth) getObsClient() (*obs.ObsClient, error) {
	return obs.New(auth.AccessKey, auth.SecretKey, auth.Endpoint)
}

type ObsClientConfig struct {
	BucketName string
	ObjectKey  string
	Location   string
}

type ObsClientService struct {
	config *ObsClientConfig
	obsClient *obs.ObsClient
}

func newObsService(auth *ObsClientAuth, config *ObsClientConfig) (*ObsClientService, error) {
	obsClient, err := auth.getObsClient()
	if err != nil {
		return nil, err
	}
	return &ObsClientService{obsClient: obsClient, config: config}, nil
}


func (service *ObsClientService) InitiateMultipartUpload() (string, error) {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = service.config.BucketName
	input.Key = service.config.ObjectKey

	output, err := service.obsClient.InitiateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	return output.UploadId, nil
}

func (service *ObsClientService) UploadPart(uploadId string, body io.Reader) (string, int, error) {
	input := &obs.UploadPartInput{}
	input.Bucket = service.config.BucketName
	input.Key = service.config.ObjectKey
	input.UploadId = uploadId
	input.PartNumber = 1
	input.Body = body
	output, err := service.obsClient.UploadPart(input)
	if err != nil {
		return "", 0, err
	}
	return output.ETag, output.PartNumber, nil
}

func (service *ObsClientService) CompleteMultipartUpload(uploadId, etag string, partNumber int) error {
	input := &obs.CompleteMultipartUploadInput{}
	input.Bucket = service.config.BucketName
	input.Key = service.config.ObjectKey
	input.UploadId = uploadId
	input.Parts = []obs.Part{
		{PartNumber: partNumber, ETag: etag},
	}
	_, err := service.obsClient.CompleteMultipartUpload(input)
	if err != nil {
		return err
	}
	fmt.Printf("Upload object %s successfully!\n", service.config.ObjectKey)
	return nil
}

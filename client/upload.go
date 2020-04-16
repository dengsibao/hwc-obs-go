package client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"time"
)

func Upload(header *multipart.FileHeader, directory string, auth *ObsClientAuth, config *ObsClientConfig) (string, error) {
	file, err := header.Open()
	if err != nil {
		log.Printf("Open file fail: %v", err)
		return "", err
	}

	defer file.Close()

	byteArr, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return UploadByByteArr(byteArr, header.Filename, directory, auth, config)
}

func getObsObjectKey(directory string, filename string) string {
	now := time.Now()
	fileName := fmt.Sprintf("%s/%s/%s/%s", directory, now.Format("20060102"), GainRandomString(12), filename)
	return fileName
}

func UploadByByteArr(byteArr []byte, filename string, directory string,
	auth *ObsClientAuth, config *ObsClientConfig) (string, error) {

	var objectKey = getObsObjectKey(directory, filename)
	service, err := newObsService(auth, config)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Step 1: initiate multipart upload")
	uploadId, err := service.InitiateMultipartUpload()
	if err != nil {
		return "", err
	}

	fmt.Println("Step 2: upload a part")
	etag, partNumber, err := service.UploadPart(uploadId, bytes.NewReader(byteArr))
	if err != nil {
		return "", err
	}
	fmt.Println("Step 3: complete multipart upload")
	err = service.CompleteMultipartUpload(uploadId, etag, partNumber)
	if err != nil {
		return "", err
	}

	var urlPrefix = fmt.Sprintf("https://%v.%v", config.BucketName, auth.Endpoint)


	return fmt.Sprintf("%v/%v", urlPrefix, objectKey), nil
}


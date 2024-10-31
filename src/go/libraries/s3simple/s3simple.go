package s3simple

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

//  B2 Examples:
//  https://www.backblaze.com/docs/cloud-storage-use-the-aws-sdk-for-go-with-backblaze-b2

func Setup(key string, secret string, endpoint string, region string) s3.S3 {
	//	fmt.Printf("Setup: key: %s, secret: %s, endpoint: %s, region: %s", key, secret, endpoint, region)
	sess := s3.New(session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(key, secret, ""),
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
	})))

	return *sess
}

func PutBlock(block string, data []byte, bucket string, sess *s3.S3, overwrite bool) error {

	if overwrite == false {
		have, err1 := HaveFile(block, bucket, sess)
		if err1 == nil && have {
			//fmt.Printf("Skipped existing block %s/%s\n", bucket, block)
			return nil
		}
	}

	reader := bytes.NewReader(data)
	var err error
	_, err = sess.PutObject(&s3.PutObjectInput{
		Body:   reader,
		Bucket: aws.String(bucket),
		Key:    aws.String(block),
	})
	if err != nil {
		//fmt.Printf("Failed to upload object /%s/%s, %s\n", bucket, block, err.Error())
		return err
	} else {
		//fmt.Printf("Uploaded block %s/%s\n", bucket, block)
	}
	return nil

}

func HaveFile(fullPath string, bucket string, sess *s3.S3) (bool, error) {

	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fullPath),
	}

	_, err := sess.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			fmt.Println("Not found- we got the NoSuchKey")
			return false, nil
		} else if aerr.Code() == "NotFound" {
			return false, nil
		} else {
			return false, aerr
		}
	} else {
		return true, nil
	}

}

func ListEverything(bucket string, sess *s3.S3) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	var objects []*s3.Object
	err := sess.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			objects = append(objects, page.Contents...)
			return true // continue paging
		})

	if err != nil {
		return nil, err
	}

	var items []string
	for _, obj := range objects {
		items = append(items, *obj.Key)
	}

	return items, nil
}

func ListBucket(prefix string, excludePrefix string, bucket string, sess *s3.S3) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	var objects []*s3.Object
	err := sess.ListObjectsV2Pages(input,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				if !strings.HasPrefix(*obj.Key, excludePrefix) {
					objects = append(objects, obj)
				}
			}
			return true // continue paging
		})

	if err != nil {
		return nil, err
	}

	var items []string
	//fmt.Printf("Objects in %s excluding %s:\n", bucket, excludePrefix)
	for _, obj := range objects {
		//fmt.Println(*obj.Key)
		items = append(items, *obj.Key)
	}

	return items, nil
}

func GetFile(fullPath string, bucket string, sess *s3.S3) ([]byte, error) {

	result, err := sess.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fullPath),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}
	defer result.Body.Close()

	// Read the object's body into a byte slice
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object body: %w", err)
	}

	return buf.Bytes(), nil

}

func DeleteFile(fullPath string, bucket string, sess *s3.S3) error {
	_, err := sess.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fullPath),
	})
	if err != nil {
		//		fmt.Println("Failed to delete file", err)
		return err
	}
	//	fmt.Printf("Successfully DELETED key %s\n", fullPath)
	return nil
}

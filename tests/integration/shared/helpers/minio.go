package helpers

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v7"
	minio_creds "github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioHelper struct {
	Endpoint     string
	RootUser     string
	RootPassword string
}

func (m *MinioHelper) InitBucketAndUser(bucket string, region string, userName string, userPassword string, isGetOnly bool, replaceBucketIfExisting bool) error {
	ctx := context.Background()
	minioAdminClient, err := madmin.New(m.Endpoint, m.RootUser, m.RootPassword, false)
	if err != nil {
		return err
	}
	minioClient, err := minio.New(m.Endpoint, &minio.Options{
		Creds:  minio_creds.NewStaticV4(m.RootUser, m.RootPassword, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}

	if replaceBucketIfExisting {
		// list and delete all keys in bucket
		bucketExists, err := minioClient.BucketExists(ctx, bucket)
		if err != nil {
			return err
		}
		if bucketExists {
			for obj := range (minioClient.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})) {
				err = minioClient.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{ForceDelete: true})
				if err != nil {
					return err
				}
			}
		}

		// attempt to delete bucket if already existing
		err = minioClient.RemoveBucket(ctx, bucket)
		if err != nil && !strings.Contains(err.Error(), "bucket does not exist") {
			return err
		}
	}

	// create bucket
	err = minioClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
	if err != nil && !strings.Contains(err.Error(), "previous request to create the named bucket succeeded") {
		return err
	}

	// attempt to delete user if already existing
	err = minioAdminClient.RemoveUser(ctx, userName)
	if err != nil && !strings.Contains(err.Error(), "specified user does not exist") {
		return err
	}

	// attempt to delete policy if already existng
	policyName := fmt.Sprintf("%s-policy", userName)
	// attempt to delete policy
	err = minioAdminClient.RemoveCannedPolicy(ctx, policyName)
	if err != nil {
		return err
	}

	// create user
	err = minioAdminClient.AddUser(ctx, userName, userPassword)
	if err != nil {
		return err
	}

	// create policy
	if isGetOnly {
		policy := []byte(fmt.Sprintf(`{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject"],"Effect": "Allow","Resource": "arn:aws:s3:::%s/*"}]}`, bucket))
		err = minioAdminClient.AddCannedPolicy(ctx, policyName, policy)
		if err != nil {
			return err
		}
	} else {
		// matches Aviary policy in AWS as of 2022-01-22
		policy := []byte(fmt.Sprintf(`
		{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Effect": "Allow",
					"Action": ["s3:ListBucket","s3:GetBucketLocation","s3:*Multipart*"],
					"Resource": "arn:aws:s3:::*"
				},
				{
					"Effect": "Allow",
					"Action": ["s3:PutObject","s3:ListBucket","s3:GetObject","s3:DeleteObject"],
					"Resource": "arn:aws:s3:::%s/*"
				}
			]
		}
		`, bucket))
		err = minioAdminClient.AddCannedPolicy(ctx, policyName, policy)
		if err != nil {
			return err
		}
	}

	// set policy on user
	err = minioAdminClient.SetPolicy(ctx, policyName, userName, false)
	if err != nil {
		return err
	}
	return nil
}

func (m *MinioHelper) AddFileToBucket(filePath string, bucket string, region string, key string) error {
	ctx := context.Background()
	minioClient, err := minio.New(m.Endpoint, &minio.Options{
		Creds:  minio_creds.NewStaticV4(m.RootUser, m.RootPassword, ""),
		Region: region,
		Secure: false,
	})
	if err != nil {
		return err
	}
	_, err = minioClient.FPutObject(ctx, bucket, key, filePath, minio.PutObjectOptions{})
	return err
}

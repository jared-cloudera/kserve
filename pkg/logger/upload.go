package logger

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	awsCreds "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/kserve/kserve/pkg/agent/storage"
	"github.com/kserve/kserve/pkg/credentials"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func GetLoggerConfig(logCredentialsFile string, log *zap.SugaredLogger) (*credentials.LoggerConfig, error) {
	credFile, err := os.Open(logCredentialsFile)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Infof("Error closing logger credentials file:", err)
		}
	}(credFile)

	credFileStat, err := credFile.Stat()
	if err != nil {
		log.Errorw("Error getting logger credentials file stat:", err)
	}
	credBuf := make([]byte, credFileStat.Size())
	_, err = credFile.Read(credBuf)

	var loggerCredentials credentials.LoggerConfig
	err = yaml.Unmarshal(credBuf, &loggerCredentials)
	if err != nil {
		return nil, err
	}
	log.Infow("Loaded logger credentials file", "logCredentialsFile", logCredentialsFile)
	return &loggerCredentials, nil
}

func UploadObjectToS3(loggerConfig *credentials.LoggerConfig, log *zap.SugaredLogger, logUrl *url.URL, objectName string, value []byte) error {
	awsConfig := &aws.Config{
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}
	awsConfig.WithCredentials(awsCreds.NewStaticCredentials(loggerConfig.S3.S3AccessKeyIDName, loggerConfig.S3.S3SecretAccessKeyName, ""))
	awsConfig.Endpoint = aws.String(loggerConfig.S3.S3Endpoint)

	sess, err := session.NewSession(awsConfig)

	// split logUrl into bucket and key, with optional s3 prefix
	s3Uri := strings.TrimPrefix(logUrl.String(), string(storage.S3))
	tokens := strings.SplitN(s3Uri, "/", 2)
	bucket := tokens[0]
	key := filepath.Join(tokens[1:]...)

	now := time.Now().Nanosecond()
	s3Client := s3.New(sess)

	uploader := storage.S3ObjectUploader{
		Bucket:   bucket,
		Prefix:   key,
		Uploader: s3manager.NewUploaderWithClient(s3Client, func(u *s3manager.Uploader) {}),
	}
	uploader.UploadObject(bucket, fmt.Sprintf(objectName, key, now), value)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Info("Successfully uploaded object to S3")
	return nil
}

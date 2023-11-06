package filestore

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type PATHTYPE int

const (
	FILE PATHTYPE = iota
	FOLDER
)

const (
	DEFAULTMAXKEYS   int32  = 1000
	DEFAULTDELIMITER string = "/"
)

var chunkSize int64 = 10 * 1024 * 1024

// Path config a PATH or array of PATHS
// representing a resource.  Array of PATHS
// is used to support multi-file resources
// such as geospatial shape files
type PathConfig struct {
	Path  string
	Paths []string
}

type FileOperationOutput struct {

	//AWS Etag for S3 results.  MD5 hash for file system operations
	ETag string
}

type FileStoreResultObject struct {
	ID         int       `json:"id"`
	Name       string    `json:"fileName"`
	Size       string    `json:"size"`
	Path       string    `json:"filePath"`
	Type       string    `json:"type"`
	IsDir      bool      `json:"isdir"`
	Modified   time.Time `json:"modified"`
	ModifiedBy string    `json:"modifiedBy"`
}

type UploadConfig struct {

	//Path to the object being uploaded into
	ObjectPath string

	//upload chunk number
	ChunkId int32

	//GUID for the file upload identifier
	UploadId string

	//chunk data
	Data []byte
}

type CompletedObjectUploadConfig struct {

	//GUID for the file upload identifier
	UploadId string

	//Path to the object being uploaded into
	ObjectPath string

	//ETags for uploaded parts
	ChunkUploadIds []string
}

type UploadResult struct {
	ID         string `json:"id"`
	WriteSize  int    `json:"size"`
	IsComplete bool   `json:"isComplete"`
}

type FileVisitFunction func(path string, file os.FileInfo) error
type ProgressFunction func(pd ProgressData)

type ProgressData struct {
	Index int
	Max   int
	Value any
}

type GetObjectInput struct {

	//Path to resource
	Path PathConfig

	//Downloads the specified range bytes of an object. Uses rfc9110 syntax
	// https://www.rfc-editor.org/rfc/rfc9110.html#name-range
	//Note: Does not support multiple ranges in a single request
	Range string
}

type PutObjectInput struct {
	Source   ObjectSource
	Dest     PathConfig
	Mutipart bool
	PartSize int
}

type ObjectSource struct {

	//optional content length.  Will be determined automatically for byte slice sources (i.e. Data)
	ContentLength int64

	//One of the next three sources must be provided
	//an existing io.ReadCloser
	Reader io.ReadCloser

	//a byte slice of data
	Data []byte

	//a file path to a resource
	Filepath PathConfig
}

func (obs *ObjectSource) ReadCloser() (io.ReadCloser, error) {
	if obs.Reader != nil {
		return obs.Reader, nil
	}
	if obs.Filepath.Path != "" {
		return os.Open(obs.Filepath.Path)
	}
	if obs.Data != nil {
		obs.ContentLength = int64(len(obs.Data))
		return io.NopCloser(bytes.NewReader(obs.Data)), nil
	}
	return nil, errors.New("Invalid ObjectSource configuration")
}

type DeleteObjectInput struct {
	Path     PathConfig
	Progress ProgressFunction
}

type WalkInput struct {
	Path     PathConfig
	Progress ProgressFunction
}

type CopyObjectInput struct {
	Src      PathConfig
	Dest     PathConfig
	Progress ProgressFunction
}

type FileStore interface {
	//requests a slice of resources at a store directory
	GetDir(path PathConfig) (*[]FileStoreResultObject, error)

	//gets io/fs FileInfo for the resource
	GetObjectInfo(PathConfig) (fs.FileInfo, error)

	//gets a readcloser for the resource.
	//caller is responsible for closing the resource
	GetObject(GetObjectInput) (io.ReadCloser, error)

	//returns a resource name for the store.
	//refer to individual implementations for details on the resource name
	ResourceName() string

	//Put (upload) an object
	PutObject(PutObjectInput) (*FileOperationOutput, error)

	//copy an object in a filestore
	CopyObject(input CopyObjectInput) error

	//initialize a multipart upload sessions
	InitializeObjectUpload(UploadConfig) (UploadResult, error)

	//write a chunk in a multipart upload session
	WriteChunk(UploadConfig) (UploadResult, error)

	//complete a multipart upload session
	CompleteObjectUpload(CompletedObjectUploadConfig) error

	//recursively deletes objects matching the path pattern
	DeleteObjects(DeleteObjectInput) []error

	//Walk a filestore starting at a given path
	//FileVisitFunction will be called for each object identified in the path
	Walk(WalkInput, FileVisitFunction) error
}

func NewFileStore(fsconfig interface{}) (FileStore, error) {
	switch scType := fsconfig.(type) {
	case BlockFSConfig:
		fs := BlockFS{}
		return &fs, nil
	case S3FSConfig:
		var cfg aws.Config
		var err error
		maxKeys := DEFAULTMAXKEYS
		if scType.MaxKeys > 0 {
			maxKeys = scType.MaxKeys
		}
		delimiter := DEFAULTDELIMITER
		if scType.Delimiter != "" {
			delimiter = scType.Delimiter
		}
		loadOptions := []func(*config.LoadOptions) error{}
		if scType.AwsOptions != nil {
			loadOptions = append(loadOptions, scType.AwsOptions...)
		}
		loadOptions = append(loadOptions, config.WithRegion(scType.S3Region))
		/////AWS RETRY OPTION
		/*
			loadOptions = append(loadOptions, config.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxBackoffDelay(retry.NewStandard(), time.Second*5)
			}))
		*/
		////
		switch cred := scType.Credentials.(type) {
		case S3FS_Static:
			loadOptions = append(loadOptions, config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(cred.S3Id, cred.S3Key, ""),
			))
		case S3FS_Attached:
			if cred.Profile != "" {
				loadOptions = append(loadOptions, config.WithSharedConfigProfile(cred.Profile))
			}
		case S3FS_Role:
			return nil, errors.New("Assumed rules are not supported")

		default:
			return nil, errors.New("Invalid S3 Credentials")
		}

		cfg, err = config.LoadDefaultConfig(
			context.TODO(),
			loadOptions...,
		)

		if err != nil {
			return nil, err
		}

		s3Client := s3.NewFromConfig(cfg)
		fs := S3FS{
			s3client:  s3Client,
			config:    &scType,
			delimiter: delimiter,
			maxKeys:   maxKeys,
		}
		return &fs, nil

	default:
		return nil, errors.New(fmt.Sprintf("Invalid File System System Type Configuration: %v", scType))
	}
}

//config.WithSharedConfigProfile("my-account-name"))

type PathParts struct {
	Parts []string
}

func (pp PathParts) ToPath(additionalParts ...string) string {
	parts := append(pp.Parts, additionalParts...)
	return buildUrl(parts, FOLDER)
}

func (pp PathParts) ToFilePath(additionalParts ...string) string {
	parts := append(pp.Parts, additionalParts...)
	return buildUrl(parts, FILE)
}

func sanitizePath(path string) string {
	return strings.ReplaceAll(path, "..", "")
}

// @TODO this is duplicated!!!!
func buildUrl(urlparts []string, pathType PATHTYPE) string {
	var b strings.Builder
	t := "/%s"
	for _, p := range urlparts {
		p = strings.Trim(strings.ReplaceAll(p, "//", "/"), "/")
		//p = strings.Trim(p, "/")
		if p != "" {
			fmt.Fprintf(&b, t, p)
		}
	}
	if pathType == FOLDER {
		fmt.Fprintf(&b, "%s", "/")
	}
	return sanitizePath(b.String())
}

func getFileMd5(f *os.File) string {
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().IsDir()
}

package filestore

import (
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

type PathConfig struct {
	Path  string
	Paths []string
}

type FileOperationOutput struct {
	Md5 string
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
	//PathInfo   models.ModelPathInfo
	//DirPath    string
	//FilePath   string
	ObjectPath string
	//ObjectName string
	ChunkId int32
	//FileId     uuid.UUID
	UploadId string
	Data     []byte
}

type CompletedObjectUploadConfig struct {
	UploadId string
	//PathInfo       models.ModelPathInfo
	//DirPath        string
	//FilePath       string
	ObjectPath string
	//ObjectName     string
	ChunkUploadIds []string
}

type UploadResult struct {
	ID         string `json:"id"`
	WriteSize  int    `json:"size"`
	IsComplete bool   `json:"isComplete"`
}

type FileVisitFunction func(path string, file os.FileInfo) error
type FileWalkCompleteFunction func() error
type ProgressFunction func(pd ProgressData)
type ProgressData struct {
	Index int
	Max   int
	Value any
}

// @TODO evaluate PathConfig as an input.  should this just be a string path.....
type FileStore interface {
	GetDir(path PathConfig) (*[]FileStoreResultObject, error)
	GetObject(PathConfig) (io.ReadCloser, error)
	GetObjectInfo(PathConfig) (fs.FileInfo, error)
	PutObject(PathConfig, []byte) (*FileOperationOutput, error)
	CopyObject(source PathConfig, dest PathConfig) error
	ResourceName() string
	Upload(reader io.Reader, key string) error
	UploadFile(filepth string, key string) error
	InitializeObjectUpload(UploadConfig) (UploadResult, error)
	WriteChunk(UploadConfig) (UploadResult, error)
	CompleteObjectUpload(CompletedObjectUploadConfig) error
	DeleteObject(path string) error //depricate eventually?
	DeleteObjects(path PathConfig) error
	Walk(string, FileVisitFunction) error
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
		loadOptions = append(loadOptions, config.WithRegion(scType.S3Region))
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

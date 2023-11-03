package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const max_copy_chunk_size = 5 * 1024 * 1024
const max_put_object_copy_size = 5000 * 1024 * 1024

type S3AttributesFileInfo struct {
	name string
	*s3.GetObjectAttributesOutput
}

func (obj *S3AttributesFileInfo) Name() string {
	return obj.name
}

func (obj *S3AttributesFileInfo) Size() int64 {
	return obj.ObjectSize
}

func (obj *S3AttributesFileInfo) Mode() os.FileMode {
	return os.ModeIrregular
}

func (obj *S3AttributesFileInfo) ModTime() time.Time {
	return time.Now()
}

func (obj *S3AttributesFileInfo) IsDir() bool {
	return false
}

func (obj *S3AttributesFileInfo) Sys() interface{} {
	return nil
}

type S3FileInfo struct {
	s3 *types.Object
}

func (obj *S3FileInfo) Name() string {
	return *obj.s3.Key
}

func (obj *S3FileInfo) Size() int64 {
	return obj.s3.Size
}

func (obj *S3FileInfo) Mode() os.FileMode {
	return os.ModeIrregular
}

func (obj *S3FileInfo) ModTime() time.Time {
	return *obj.s3.LastModified
}

func (obj *S3FileInfo) IsDir() bool {
	return false
}

func (obj *S3FileInfo) Sys() interface{} {
	return nil
}

type S3FS_Role struct {
	ARN string
}

type S3FS_Attached struct {
	Profile string
}

type S3FS_Static struct {
	S3Id  string
	S3Key string
}

type S3FSConfig struct {
	S3Region    string
	S3Bucket    string
	Delimiter   string
	MaxKeys     int32
	Credentials any
	AwsOptions  []func(*config.LoadOptions) error
}

type S3FS struct {
	s3client                 *s3.Client
	config                   *S3FSConfig
	delimiter                string
	maxKeys                  int32
	ignoreContinuationOnWalk bool //internal use only
}

func (s3fs *S3FS) GetConfig() *S3FSConfig {
	return s3fs.config
}

func (s3fs *S3FS) ResourceName() string {
	return s3fs.config.S3Bucket
}

func (s3fs *S3FS) GetObjectInfo(path PathConfig) (fs.FileInfo, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	params := &s3.GetObjectAttributesInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
			types.ObjectAttributesObjectSize,
		},
	}

	resp, err := s3fs.s3client.GetObjectAttributes(context.TODO(), params)
	return &S3AttributesFileInfo{s3Path, resp}, err
}

// @TODO should this return an error on failure to list?  Think so!
func (s3fs *S3FS) GetDir(path PathConfig) (*[]FileStoreResultObject, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")

	shouldContinue := true
	var continuationToken *string = nil
	prefixes := []types.CommonPrefix{}
	objects := []types.Object{}

	for shouldContinue {
		params := &s3.ListObjectsV2Input{
			Bucket:            &s3fs.config.S3Bucket,
			Prefix:            &s3Path,
			Delimiter:         &s3fs.delimiter,
			MaxKeys:           s3fs.maxKeys,
			ContinuationToken: continuationToken,
		}

		resp, err := s3fs.s3client.ListObjectsV2(context.TODO(), params)
		if err != nil {
			log.Printf("failed to list objects in the bucket - %v", err)
			return nil, err
		}
		prefixes = append(prefixes, resp.CommonPrefixes...)
		objects = append(objects, resp.Contents...)
		if resp.ContinuationToken == nil {
			shouldContinue = false
		} else {
			continuationToken = resp.ContinuationToken
		}
	}

	result := []FileStoreResultObject{}
	var count int = 0
	for _, cp := range prefixes {
		w := FileStoreResultObject{
			ID:         count,
			Name:       filepath.Base(*cp.Prefix),
			Size:       "",
			Path:       *cp.Prefix,
			Type:       "",
			IsDir:      true,
			ModifiedBy: "",
		}
		count++
		result = append(result, w)
	}

	for _, object := range objects {
		w := FileStoreResultObject{
			ID:         count,
			Name:       filepath.Base(*object.Key),
			Size:       strconv.FormatInt(object.Size, 10),
			Path:       filepath.Dir(*object.Key),
			Type:       filepath.Ext(*object.Key),
			IsDir:      false,
			Modified:   *object.LastModified,
			ModifiedBy: "",
		}
		count++
		result = append(result, w)
	}

	return &result, nil
}

func (s3fs *S3FS) GetObject(goi GetObjectInput) (io.ReadCloser, error) {
	s3Path := strings.TrimPrefix(goi.Path.Path, "/")
	input := &s3.GetObjectInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
		Range:  &goi.Range,
	}
	output, err := s3fs.s3client.GetObject(context.TODO(), input)
	return output.Body, err
}

func (s3fs *S3FS) PutObject(poi PutObjectInput) (*FileOperationOutput, error) {
	s3Path := strings.TrimPrefix(poi.Dest.Path, "/")
	reader, err := poi.Source.ReadCloser()
	if err != nil {
		return nil, fmt.Errorf("Unable to get the Source Reader: %s\n", err)
	}
	defer reader.Close()
	if poi.Mutipart {
		uploader := manager.NewUploader(s3fs.s3client)
		s3output, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket: &s3fs.config.S3Bucket,
			Key:    &s3Path,
			Body:   reader,
		})
		if err != nil {
			return nil, err
		}
		output := &FileOperationOutput{
			Md5: *s3output.ETag,
		}
		return output, err
	} else {
		input := &s3.PutObjectInput{
			Bucket:        &s3fs.config.S3Bucket,
			Body:          reader,
			ContentLength: poi.Source.ContentLength,
			Key:           &s3Path,
		}
		s3output, err := s3fs.s3client.PutObject(context.TODO(), input)
		if err != nil {
			return nil, err
		}
		output := &FileOperationOutput{
			Md5: *s3output.ETag,
		}
		return output, err
	}

}

func (s3fs *S3FS) DeleteObjects(doi DeleteObjectInput) []error {

	objects := make([]types.ObjectIdentifier, 0, len(doi.Path.Paths))
	for _, p := range doi.Path.Paths {
		p := p
		s3Path := strings.TrimPrefix(p, "/")
		object := types.ObjectIdentifier{
			Key: &s3Path,
		}
		objects = append(objects, object)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: &s3fs.config.S3Bucket,
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   false,
		},
	}

	return s3fs.deleteListImpl(input, doi.Progress)

}

func (s3fs *S3FS) deleteListImpl(input *s3.DeleteObjectsInput, pf ProgressFunction) []error {
	errs := []error{}
	s3fs.ignoreContinuationOnWalk = true
	defer func() {
		s3fs.ignoreContinuationOnWalk = false
	}()
	maxDelBufferSize := 1000
	delBuffer := []types.ObjectIdentifier{}
	count := 0
	for _, obj := range input.Delete.Objects {
		//s3fs.Walk(*obj.Key, func(path string, file os.FileInfo) error {
		s3fs.Walk(WalkInput{Path: PathConfig{Path: *obj.Key}, Progress: pf}, func(path string, file os.FileInfo) error {
			key := file.Name()
			delBuffer = append(delBuffer, types.ObjectIdentifier{Key: &key})
			if len(delBuffer) >= maxDelBufferSize {
				err := s3fs.flushDeletes(delBuffer)
				if err != nil {
					log.Printf("Error in batch delete operation: %s\n", err)
				}
			}
			count++
			return nil
		})

		//flush any remaining deletes
		err := s3fs.flushDeletes(delBuffer)
		if err != nil {
			log.Printf("Error in batch delete operation: %s\n", err)
		}
	}
	return errs
}

func (s3fs *S3FS) flushDeletes(delBuffer []types.ObjectIdentifier) []error {
	if len(delBuffer) == 0 {
		return []error{errors.New("nothing to delete")}
	}
	input := &s3.DeleteObjectsInput{
		Bucket: &s3fs.config.S3Bucket,
		Delete: &types.Delete{
			Objects: delBuffer,
		},
	}
	out, err := s3fs.deleteObjectsImpl(input)
	if err != nil {
		return []error{err}
	}

	errs := make([]error, len(out.Errors))
	for i, e := range out.Errors {
		if e.Key != nil && e.Code != nil && e.Message != nil {
			errs[i] = fmt.Errorf("%s: %s: %s", *e.Key, *e.Code, *e.Message)
		} else {
			errs[i] = errors.New("Unknown AWS delete error")
		}
	}
	return errs
}

func (s3fs *S3FS) deleteObjectsImpl(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	result, err := s3fs.s3client.DeleteObjects(context.TODO(), input)
	return result, err
}

func (s3fs *S3FS) CopyObject(coi CopyObjectInput) error {
	info, err := s3fs.GetObjectInfo(coi.Src)
	if err != nil {
		return err
	}

	var fileSize int64 = info.Size()
	if fileSize < max_put_object_copy_size {
		source := fmt.Sprintf("%s/%s", s3fs.ResourceName(), strings.TrimPrefix(coi.Src.Path, "/"))
		dest := strings.TrimPrefix(coi.Dest.Path, "/")
		input := s3.CopyObjectInput{
			Bucket:     &s3fs.config.S3Bucket,
			CopySource: &source,
			Key:        &dest,
		}
		_, err = s3fs.s3client.CopyObject(context.TODO(), &input)
	} else {
		s3fs.copyPartsTo(coi.Src, coi.Dest, fileSize)
	}
	return err
}

func (s3fs *S3FS) copyPartsTo(sourcePath PathConfig, destPath PathConfig, fileSize int64) error {
	source := fmt.Sprintf("%s/%s", s3fs.ResourceName(), strings.TrimPrefix(sourcePath.Path, "/"))
	dest := strings.TrimPrefix(destPath.Path, "/")

	/*
		ctx, cancelFn := context.WithTimeout(context.TODO(), 10*time.Minute)
		defer cancelFn()
	*/
	//struct for starting a multipart upload
	destInput := s3.CreateMultipartUploadInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &dest,
	}
	var uploadId string
	createOutput, err := s3fs.s3client.CreateMultipartUpload(context.TODO(), &destInput)
	if err != nil {
		return err
	}
	if createOutput != nil {
		if createOutput.UploadId != nil {
			uploadId = *createOutput.UploadId
		}
	}
	if uploadId == "" {
		return errors.New("No upload id found in start upload request")
	}

	var i int64
	var partNumber int32 = 1
	//copySource := fmt.Sprintf("%s/%s", s3fs.config.S3Bucket, source)

	parts := make([]types.CompletedPart, 0)
	numUploads := fileSize / max_copy_chunk_size
	log.Printf("Will attempt upload in %d number of parts to %s", numUploads, dest)

	for i = 0; i < fileSize; i += max_copy_chunk_size {
		copyRange := buildCopySourceRange(i, fileSize)
		partInput := s3.UploadPartCopyInput{
			Bucket:          &s3fs.config.S3Bucket,
			CopySource:      &source,
			CopySourceRange: &copyRange,
			Key:             &dest,
			PartNumber:      partNumber,
			UploadId:        &uploadId,
		}

		partResp, err := s3fs.s3client.UploadPartCopy(context.TODO(), &partInput)

		if err != nil {
			log.Println("Attempting to abort upload")
			abortIn := s3.AbortMultipartUploadInput{
				UploadId: &uploadId,
			}
			//ignoring any errors with aborting the copy
			s3fs.s3client.AbortMultipartUpload(context.TODO(), &abortIn)
			return fmt.Errorf("Error uploading part %d : %w", partNumber, err)
		}

		//copy etag and part number from response as it is needed for completion
		if partResp != nil {
			partNum := partNumber
			etag := strings.Trim(*partResp.CopyPartResult.ETag, "\"")
			cPart := types.CompletedPart{
				ETag:       &etag,
				PartNumber: partNum,
			}
			parts = append(parts, cPart)
			log.Printf("Successfully upload part %d of %s\n", partNumber, uploadId)
		}
		partNumber++
		if partNumber%50 == 0 {
			log.Printf("Completed part %d of %d to %s\n", partNumber, numUploads, dest)
		}
	}
	//create struct for completing the upload
	mpu := types.CompletedMultipartUpload{
		Parts: parts,
	}

	//complete actual upload
	//does not actually copy if the complete command is not received
	complete := s3.CompleteMultipartUploadInput{
		Bucket:          &s3fs.config.S3Bucket,
		Key:             &dest,
		UploadId:        &uploadId,
		MultipartUpload: &mpu,
	}
	compOutput, err := s3fs.s3client.CompleteMultipartUpload(context.TODO(), &complete)
	if err != nil {
		return fmt.Errorf("Error completing upload: %w", err)
	}
	if compOutput != nil {
		log.Println("Finished copy")
	}
	return nil

}

func (s3fs *S3FS) InitializeObjectUpload(u UploadConfig) (UploadResult, error) {
	output := UploadResult{}
	s3path := u.ObjectPath //@TODO incomoplete
	s3path = strings.TrimPrefix(s3path, "/")
	input := &s3.CreateMultipartUploadInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3path,
	}

	resp, err := s3fs.s3client.CreateMultipartUpload(context.TODO(), input)
	if err != nil {
		return output, err
	}
	output.ID = *resp.UploadId
	return output, nil
}

func (s3fs *S3FS) WriteChunk(u UploadConfig) (UploadResult, error) {
	s3path := u.ObjectPath //@TODO incomplete
	s3path = strings.TrimPrefix(s3path, "/")
	partNumber := u.ChunkId + 1 //aws chunks are 1 to n, our chunks are 0 referenced
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(u.Data),
		Bucket:        &s3fs.config.S3Bucket,
		Key:           &s3path,
		PartNumber:    partNumber,
		UploadId:      &u.UploadId,
		ContentLength: int64(len(u.Data)),
	}
	result, err := s3fs.s3client.UploadPart(context.TODO(), partInput)

	if err != nil {
		return UploadResult{}, err
	}
	output := UploadResult{
		WriteSize: len(u.Data),
		ID:        *result.ETag,
	}
	return output, nil
}

func (s3fs *S3FS) CompleteObjectUpload(u CompletedObjectUploadConfig) error {
	s3path := u.ObjectPath //@TODO incomplete
	s3path = strings.TrimPrefix(s3path, "/")
	cp := []types.CompletedPart{}
	for i, cuId := range u.ChunkUploadIds {
		cp = append(cp, types.CompletedPart{
			ETag:       &cuId,
			PartNumber: int32(i + 1),
		})
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   &s3fs.config.S3Bucket,
		Key:      &s3path,
		UploadId: &u.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: cp,
		},
	}
	result, err := s3fs.s3client.CompleteMultipartUpload(context.TODO(), input)
	fmt.Print(result)
	return err
}

func (s3fs *S3FS) Walk(input WalkInput, vistorFunction FileVisitFunction) error {
	s3Path := strings.TrimPrefix(input.Path.Path, "/")
	s3delim := ""
	query := &s3.ListObjectsV2Input{
		Bucket:    &s3fs.config.S3Bucket,
		Prefix:    &s3Path,
		Delimiter: &s3delim,
		MaxKeys:   s3fs.maxKeys,
	}

	truncatedListing := true
	count := 0
	for truncatedListing {
		resp, err := s3fs.s3client.ListObjectsV2(context.TODO(), query)
		if err != nil {
			return err
		}
		for _, content := range resp.Contents {
			fileInfo := &S3FileInfo{&content}
			err = vistorFunction("/"+*content.Key, fileInfo)
			if err != nil {
				log.Printf("Visitor Function error: %s\n", err)
			}
			if input.Progress != nil {
				input.Progress(ProgressData{
					Index: count,
					Max:   -1,
					Value: fileInfo,
				})
			}
		}
		if !s3fs.ignoreContinuationOnWalk {
			query.ContinuationToken = resp.NextContinuationToken
		}
		truncatedListing = resp.IsTruncated
		count++
	}
	return nil
}

/*
these functions are not part of the filestore interface and are unique to the S3FS
*/

func (s3fs *S3FS) GetPresignedUrl(path PathConfig, days int) (string, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	presignClient := s3.NewPresignClient(s3fs.s3client)
	input := &s3.GetObjectInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
	}
	request, err := presignClient.PresignGetObject(context.TODO(), input, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(time.Duration(24*days) * time.Hour)
	})

	if err != nil {
		return "", err
	}
	return request.URL, nil
}

func (s3fs *S3FS) SetObjectPublic(path PathConfig) (string, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	acl := types.ObjectCannedACLPublicRead
	input := &s3.PutObjectAclInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
		ACL:    acl,
	}
	aclResp, err := s3fs.s3client.PutObjectAcl(context.TODO(), input)
	if err != nil {
		log.Printf("Failed to add public-read ACL on %s\n", s3Path)
		log.Println(aclResp)
	}
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s3fs.config.S3Bucket, s3Path)
	log.Println(url)
	return url, err
}

/////util functions

func buildCopySourceRange(start int64, objectSize int64) string {
	end := start + max_copy_chunk_size - 1
	if end > objectSize {
		end = objectSize - 1
	}
	startRange := strconv.FormatInt(start, 10)
	stopRange := strconv.FormatInt(end, 10)
	return "bytes=" + startRange + "-" + stopRange
}

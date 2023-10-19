package filestore

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestProfileCreds(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Attached{
			Profile: "orm-p",
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_DIR")}
	dirs, err := fs.GetDir(path)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(dirs)
}

func TestStaticCreds(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_DIR")}
	dirs, err := fs.GetDir(path)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(dirs)
}

func TestGetDir(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_DIR")}
	dirs, err := fs.GetDir(path)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(dirs)
}

func TestGetObject(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_FILE")}
	reader, err := fs.GetObject(path)
	if err != nil {
		t.Fatal(err)
	}
	buf := new(strings.Builder)
	io.Copy(buf, reader)
	// check errors
	fmt.Println(buf.String())
}

func TestGetObjectInfo(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_FILE")}
	info, err := fs.GetObjectInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(info)
}

func TestPutObject(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(os.Getenv("TEST_PUT_SRC"))
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: os.Getenv("TEST_PUT_DEST")}
	out, err := fs.PutObject(path, data)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(out.Md5)
}

func TestCopyObject(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	srcpath := PathConfig{Path: os.Getenv("AWS_BUCKET") + "/" + os.Getenv("TEST_COPY_SRC")}
	destpath := PathConfig{Path: os.Getenv("TEST_COPY_DEST")}
	err = fs.CopyObject(srcpath, destpath)
	if err != nil {
		t.Fatal(err)
	}
}

/*
func TestCopyObjectByParts(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	srcpath := PathConfig{Path: "filestore_tests/Archive.zip"}
	destpath := PathConfig{Path: "filestore_tests/Archive2.zip"}
	s3fs := fs.(*S3FS)
	err = s3fs.copyPartsTo(srcpath, destpath)
	if err != nil {
		t.Fatal(err)
	}
}
*/

func TestUpload(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	srcpath := os.Getenv("TEST_UPLOAD_SRC")
	destpath := os.Getenv("TEST_UPLOAD_DEST")
	reader, err := os.Open(srcpath)
	if err != nil {
		t.Fatal(err)
	}
	err = fs.Upload(reader, destpath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUploadFile(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	srcpath := os.Getenv("TEST_UPLOAD_SRC")
	destpath := os.Getenv("TEST_UPLOAD_DEST")
	err = fs.UploadFile(srcpath, destpath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteObject(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	path := os.Getenv("TEST_COPY_DEST")
	err = fs.DeleteObject(path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteObjects(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
	}

	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	path := PathConfig{Paths: []string{
		os.Getenv("TEST_DELETE_OBJECTS_SRC"),
		os.Getenv("TEST_COPY_DEST"),
	}}

	err = fs.DeleteObjects(path)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWalk(t *testing.T) {
	config := S3FSConfig{
		Credentials: S3FS_Static{
			S3Id:  os.Getenv("AWS_ACCESS_KEY_ID"),
			S3Key: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		},
		S3Region: os.Getenv("AWS_REGION"),
		S3Bucket: os.Getenv("AWS_BUCKET"),
		MaxKeys:  2,
	}

	fs, err := NewFileStore(config)
	fs.(*S3FS).ignoreContinuationOnWalk = true
	if err != nil {
		t.Fatal(err)
	}
	count := 0

	startpath := os.Getenv("TEST_DIR")
	err = fs.Walk(startpath, func(path string, file os.FileInfo) error {
		fmt.Println(file.Name())
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(count)
}

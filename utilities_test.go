package filestore

import (
	"fmt"
	"testing"
)

var testKey []byte = []byte("asdfasdfasdfasdfasdfasdfasdfasdf")

func TestSignUrl(t *testing.T) {
	uri := "https://test.com/path1/path2?param1=1234&param2=abcd"
	options := PresignInputOptions{
		Uri:        uri,
		SigningKey: testKey,
		Expiration: 60,
	}
	signedurl, err := PresignObject(options)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(signedurl)
}

func TestValidateUrl(t *testing.T) {
	uri := "https://test.com/path1/path2?X-Amx-Date=20231102T212114Z&X-Amx-Expiration=60&X-Amx-Signature=RT%252BRzKYhkc%252Faw40j8nuGkwTaZq4kV7APr%252BhY1wzOtQ4%253D&param1=1234&param2=abcd"
	options := PresignInputOptions{
		Uri:        uri,
		SigningKey: testKey,
	}
	if VerifySignedObject(options) {
		fmt.Println("VALID!")
	} else {
		t.Fatal("NOT VALID")
	}
}

package filestore

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	b64 "encoding/base64"
	"errors"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	signatureQueryName  string = "X-Amx-Signature"
	expirationQueryName string = "X-Amx-Expiration"
	timeQueryName       string = "X-Amx-Date"
	timeFormat          string = "20060102T150405Z"
	maxExpiration       int    = 86400 * 30 //30 days
)

type Retryer[T any] struct {
	MaxAttempts int
	MaxBackoff  float64
	R           float64
}

func (r Retryer[T]) Send(sendFunction func() (T, error)) (T, error) {
	attempts := 0
	for {
		t, err := sendFunction()
		if err == nil || attempts > r.MaxAttempts {
			return t, err
		}
		b := rand.Float64()
		secondsToSleep := math.Min(b*math.Pow(r.R, float64(attempts)), r.MaxBackoff)
		time.Sleep(time.Second * time.Duration(secondsToSleep))
		attempts++
	}
}

func Count(fs FileStore, dirpath string) (int64, error) {
	var count int64 = 0
	err := fs.Walk(WalkInput{Path: PathConfig{Path: dirpath}}, func(path string, file os.FileInfo) error {
		count++
		return nil
	})
	if err != nil {
		return -1, err
	}
	return count, nil
}

type PresignInputOptions struct {
	Uri        string
	SigningKey []byte
	Expiration int
}

func sign(data []byte, signingKey []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, signingKey)
	_, err := mac.Write(data)
	if err != nil {
		return nil, err
	}
	return mac.Sum(nil), nil
}

func PresignObject(options PresignInputOptions) (string, error) {
	if options.Expiration > maxExpiration {
		return "", errors.New("Expiration time too long")
	}
	uri, err := url.Parse(options.Uri)
	if err != nil {
		return "", err
	}
	qp := uri.Query()
	qp.Add(timeQueryName, time.Now().UTC().Format(timeFormat))
	qp.Add(expirationQueryName, strconv.Itoa(options.Expiration))
	uri.RawQuery = qp.Encode()
	signature, err := sign([]byte(uri.String()), options.SigningKey)
	sEnc := b64.StdEncoding.EncodeToString(signature)
	sUrl := url.QueryEscape(sEnc)
	qp.Add(signatureQueryName, sUrl)
	uri.RawQuery = qp.Encode()
	return uri.String(), nil
}
func VerifySignedObject(options PresignInputOptions) bool {
	uri, err := url.Parse(options.Uri)
	if err != nil {
		return false
	}
	sigok := verifySignature(uri, options.SigningKey)
	timeok := verifyExpiration(uri.Query())
	return sigok && timeok
}

func verifySignature(uri *url.URL, key []byte) bool {
	qp := uri.Query()
	urlSignature := qp.Get(signatureQueryName)
	b64signature, err := url.QueryUnescape(urlSignature)
	if err != nil {
		return false
	}
	signature, err := b64.StdEncoding.DecodeString(b64signature)
	if err != nil {
		return false
	}
	qp.Del(signatureQueryName)
	uri.RawQuery = qp.Encode()
	expectedSignature, err := sign([]byte(uri.String()), key)
	if err != nil {
		return false
	}
	return bytes.Equal(signature, expectedSignature)
}

func verifyExpiration(qp url.Values) bool {
	t, err := time.Parse(timeFormat, qp.Get(timeQueryName))
	if err != nil {
		return false
	}
	d, err := strconv.Atoi(qp.Get(expirationQueryName))
	if err != nil {
		return false
	}
	t = t.Add(time.Second * time.Duration(d))
	return t.After(time.Now().UTC())

}

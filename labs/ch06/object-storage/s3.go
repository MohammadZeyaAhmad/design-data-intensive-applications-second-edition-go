package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type S3Config struct {
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	AccessKey    string `json:"-"`
	SecretKey    string `json:"-"`
	SessionToken string `json:"-"`
}

var ownedPrefix = regexp.MustCompile(`^atlas-object-lab/[0-9a-f]{32}/$`)
var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

type S3Store struct {
	client *s3.Client
	config S3Config
	wire   *wireMeter
}

func NewS3Store(c S3Config) (*S3Store, error) {
	if c.Region == "" || !bucketPattern.MatchString(c.Bucket) || !ownedPrefix.MatchString(c.Prefix) || c.AccessKey == "" || c.SecretKey == "" {
		return nil, ErrInvalid
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, ErrInvalid
		}
		local := u.Hostname() == "localhost" || (net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback())
		if u.Host == "" || (u.Scheme != "https" && !(u.Scheme == "http" && local)) {
			return nil, ErrInvalid
		}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 5 * time.Second
	wire := &wireMeter{next: transport}
	options := s3.Options{Region: c.Region, Credentials: credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, c.SessionToken), Retryer: aws.NopRetryer{}, UsePathStyle: true, HTTPClient: &http.Client{Transport: wire, Timeout: 8 * time.Second}, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired}
	if c.Endpoint != "" {
		options.BaseEndpoint = aws.String(strings.TrimSuffix(c.Endpoint, "/"))
	}
	return &S3Store{client: s3.New(options), config: c, wire: wire}, nil
}
func (s *S3Store) WireCounts() WireCounts { return s.wire.snapshot() }
func s3Error(err error, write bool) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return ErrNotFound
		case "PreconditionFailed", "ConditionalRequestConflict":
			return ErrConflict
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken":
			return ErrAccess
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) {
		switch response.HTTPStatusCode() {
		case 404:
			return ErrNotFound
		case 409, 412:
			return ErrConflict
		case 401, 403:
			return ErrAccess
		}
	}
	// Do not surface SDK errors containing endpoint, signed URL or credentials.
	if write {
		return ErrUncertain
	}
	return errors.New("S3 read request failed")
}
func (s *S3Store) Get(ctx context.Context, key string) (Object, error) {
	if !validKey(key) {
		return Object{}, ErrInvalid
	}
	o, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.config.Prefix + key)})
	if err != nil {
		return Object{}, s3Error(err, false)
	}
	defer o.Body.Close()
	if o.ContentLength != nil && *o.ContentLength > MaxObjectBytes {
		return Object{}, ErrLimit
	}
	body, err := io.ReadAll(io.LimitReader(o.Body, MaxObjectBytes+1))
	if err != nil {
		return Object{}, errors.New("S3 response body interrupted")
	}
	if len(body) > MaxObjectBytes || o.ETag == nil || *o.ETag == "" {
		return Object{}, ErrCorrupt
	}
	return Object{body, *o.ETag}, nil
}
func (s *S3Store) Put(ctx context.Context, key string, body []byte, c Condition) (string, error) {
	if !validKey(key) || !c.valid() || len(body) > MaxObjectBytes {
		return "", ErrInvalid
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.config.Prefix + key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json")}
	if c.Absent {
		in.IfNoneMatch = aws.String("*")
	} else {
		in.IfMatch = aws.String(c.Version)
	}
	o, err := s.client.PutObject(ctx, in)
	if err != nil {
		return "", s3Error(err, true)
	}
	if o.ETag == nil || *o.ETag == "" {
		return "", ErrUncertain
	}
	return *o.ETag, nil
}
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	if prefix != "" && !validKey(strings.TrimSuffix(prefix, "/")) {
		return nil, ErrInvalid
	}
	keys := []string{}
	var continuation *string
	seen := map[string]bool{}
	for {
		o, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.config.Bucket), Prefix: aws.String(s.config.Prefix + prefix), ContinuationToken: continuation, MaxKeys: aws.Int32(100)})
		if err != nil {
			return nil, s3Error(err, false)
		}
		for _, object := range o.Contents {
			if object.Key == nil || !strings.HasPrefix(*object.Key, s.config.Prefix) {
				return nil, ErrCorrupt
			}
			key := strings.TrimPrefix(*object.Key, s.config.Prefix)
			if !validKey(key) {
				return nil, ErrCorrupt
			}
			keys = append(keys, key)
			if len(keys) > MaxObjects {
				return nil, ErrLimit
			}
		}
		if !aws.ToBool(o.IsTruncated) {
			break
		}
		token := aws.ToString(o.NextContinuationToken)
		if token == "" || seen[token] {
			return nil, ErrCorrupt
		}
		seen[token] = true
		continuation = aws.String(token)
	}
	sort.Strings(keys)
	return keys, nil
}
func (s *S3Store) Delete(ctx context.Context, key string) error {
	if !validKey(key) {
		return ErrInvalid
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.config.Bucket), Key: aws.String(s.config.Prefix + key)})
	return s3Error(err, true)
}

// CreateFixtureBucket is only for a fresh disposable emulator process. The
// explicit cloud target requires an existing user-supplied bucket instead.
func (s *S3Store) CreateFixtureBucket(ctx context.Context) error {
	in := &s3.CreateBucketInput{Bucket: aws.String(s.config.Bucket)}
	if s.config.Region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(s.config.Region)}
	}
	_, err := s.client.CreateBucket(ctx, in)
	return s3Error(err, true)
}
func (s *S3Store) CheckBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.config.Bucket)})
	return s3Error(err, false)
}

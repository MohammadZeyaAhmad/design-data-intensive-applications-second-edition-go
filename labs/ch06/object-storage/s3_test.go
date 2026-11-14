package objectstorage

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestS3ConditionsAndAmbiguousWritesHaveNoHiddenRetry(t *testing.T) {
	var calls atomic.Int64
	var unavailable atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if unavailable.Load() {
			if r.Header.Get("If-None-Match") != "*" || r.Header.Get("If-Match") != "" {
				t.Error("create lost its condition")
			}
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code></Error>`))
			return
		}
		if r.Header.Get("If-Match") != `"opaque-etag"` || r.Header.Get("If-None-Match") != "" {
			t.Error("replacement lost its opaque condition")
		}
		w.WriteHeader(412)
		_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
	}))
	defer server.Close()
	s, err := NewS3Store(S3Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket", Prefix: "atlas-object-lab/00000000000000000000000000000000/", AccessKey: "disposable-access", SecretKey: "disposable-secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(context.Background(), "head.json", []byte("new"), Condition{Version: `"opaque-etag"`})
	if !errors.Is(err, ErrConflict) || calls.Load() != 1 {
		t.Fatal("CAS conflict classification or hidden retry", err)
	}
	unavailable.Store(true)
	_, err = s.Put(context.Background(), "head.json", []byte("new"), Condition{Absent: true})
	if !errors.Is(err, ErrUncertain) || calls.Load() != 2 {
		t.Fatal("ambiguous write was retried inside SDK", err)
	}
}
func TestS3ListRejectsRepeatedContinuationToken(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated><NextContinuationToken>repeated</NextContinuationToken></ListBucketResult>`))
	}))
	defer server.Close()
	s, err := NewS3Store(S3Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "test-bucket", Prefix: "atlas-object-lab/00000000000000000000000000000000/", AccessKey: "disposable-access", SecretKey: "disposable-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.List(context.Background(), ""); !errors.Is(err, ErrCorrupt) || calls.Load() != 2 {
		t.Fatal("unbounded or unchecked continuation", err, calls.Load())
	}
}

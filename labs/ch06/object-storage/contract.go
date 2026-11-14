package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type ContractReport struct {
	Assertions             []string `json:"assertions"`
	ConcurrentCreators     int      `json:"concurrent_creators"`
	CreatorWins            int64    `json:"creator_wins"`
	ConcurrentReplacements int      `json:"concurrent_replacements"`
	ReplacementWins        int64    `json:"replacement_wins"`
	PaginatedObjects       int      `json:"paginated_objects"`
}

// CheckContract exercises actual conditional operations, including concurrent
// creates/replacements and multi-page listing. All keys stay in contract/.
func CheckContract(ctx context.Context, s Store) (r ContractReport, resultErr error) {
	r = ContractReport{ConcurrentCreators: 16, ConcurrentReplacements: 16, PaginatedObjects: 105}
	key := "contract/current.json"
	if _, err := s.Get(ctx, key); err == nil {
		return r, errors.New("contract namespace must be empty")
	} else if !errors.Is(err, ErrNotFound) {
		return r, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15e9)
		defer cancel()
		keys, err := s.List(cleanup, "contract/")
		if err == nil {
			for _, key := range keys {
				if e := s.Delete(cleanup, key); e != nil {
					err = errors.Join(err, e)
				}
			}
		}
		if err != nil {
			resultErr = errors.Join(resultErr, errors.New("owned contract-prefix cleanup failed"), err)
		}
	}()
	var wins atomic.Int64
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	start := make(chan struct{})
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Put(ctx, key, []byte(fmt.Sprint(i)), Condition{Absent: true})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				failures <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		return r, err
	}
	r.CreatorWins = wins.Load()
	if r.CreatorWins != 1 {
		return r, errors.New("create-if-absent did not choose exactly one winner")
	}
	base, err := s.Get(ctx, key)
	if err != nil {
		return r, err
	}
	wins.Store(0)
	failures = make(chan error, 16)
	start = make(chan struct{})
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Put(ctx, key, []byte(fmt.Sprint("new-", i)), Condition{Version: base.Version})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				failures <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		return r, err
	}
	r.ReplacementWins = wins.Load()
	if r.ReplacementWins != 1 {
		return r, errors.New("same-version CAS silently overwrote a winner")
	}
	if _, err = s.Put(ctx, key, []byte("stale"), Condition{Version: base.Version}); !errors.Is(err, ErrConflict) {
		return r, errors.New("stale CAS accepted")
	}
	current, err := s.Get(ctx, key)
	if err != nil {
		return r, err
	}
	original := string(current.Body)
	if len(current.Body) > 0 {
		current.Body[0] ^= 1
	}
	again, err := s.Get(ctx, key)
	if err != nil || string(again.Body) != original {
		return r, errors.New("returned body aliases store state")
	}
	for i := range r.PaginatedObjects {
		if _, err = s.Put(ctx, fmt.Sprintf("contract/page/%03d.json", i), []byte("page"), Condition{Absent: true}); err != nil {
			return r, err
		}
	}
	keys, err := s.List(ctx, "contract/page/")
	if err != nil || len(keys) != r.PaginatedObjects {
		return r, errors.New("paginated prefix inventory incomplete")
	}
	if _, err = s.Put(ctx, "../escape", nil, Condition{Absent: true}); !errors.Is(err, ErrInvalid) {
		return r, errors.New("invalid key accepted")
	}
	if _, err = s.Put(ctx, "contract/unconditional", nil, Condition{}); !errors.Is(err, ErrInvalid) {
		return r, errors.New("unconditional put accepted")
	}
	r.Assertions = []string{"one winner for concurrent create-if-absent", "one winner for concurrent same-version replacement", "stale replacement refused", "returned bytes do not alias durable objects", "complete multi-page prefix listing", "invalid keys and unconditional puts refused"}
	return r, nil
}

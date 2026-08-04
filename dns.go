package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/libdns/libdns"
	etcd "go.etcd.io/etcd/client/v3"
)

var (
	_ libdns.RecordAppender = Plugin{}
	_ libdns.RecordDeleter  = Plugin{}
)

func (p Plugin) AppendRecords(ctx context.Context, zone string, recs []libdns.Record) ([]libdns.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, rec := range recs {
		if r, err := rec.RR().Parse(); err != nil {
			return nil, fmt.Errorf("error parsing record: %w", err)
		} else {
			rec = r
		}

		var opts []etcd.OpOption
		if rec.RR().TTL > 0 {
			lease, err := etcd.NewLease(p.client).
				Grant(ctx, int64(rec.RR().TTL.Seconds()))
			if err != nil {
				return nil, fmt.Errorf("error granting lease: %w", err)
			}

			opts = append(opts, etcd.WithLease(lease.ID))
		}

		if _, err := p.client.KV.Put(ctx,
			p.fullName(zone, rec),
			rec.RR().Data, opts...); err != nil {
			return nil, fmt.Errorf("error putting record in etcd: %w", err)
		}
	}

	return recs, nil
}

func (p Plugin) DeleteRecords(ctx context.Context, zone string, recs []libdns.Record) ([]libdns.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var deleted []libdns.Record
	for _, rec := range recs {
		if r, err := rec.RR().Parse(); err != nil {
			return nil, fmt.Errorf("error parsing record: %w", err)
		} else {
			rec = r
		}

		if _, err := p.client.KV.Delete(ctx, p.fullName(zone, rec)); err != nil {
			return nil, fmt.Errorf("error deleting record from etcd: %w", err)
		}

		deleted = append(deleted, rec)
	}

	return deleted, nil
}

// fullName is the etcd key a record is stored under.
//
// TXT records get a further segment derived from the value, so that one name
// can hold several of them. An ACME DNS-01 challenge for both a domain and its
// wildcard needs exactly that: the two authorizations are validated at the same
// _acme-challenge name, with different values that have to be present at once.
// Keying on the value rather than an index also makes appending idempotent,
// re-presenting the same value overwrites its own key instead of accumulating
// duplicates.
//
// Every other type keeps a single key per name, so their layout is unchanged.
func (p Plugin) fullName(zone string, rec libdns.Record) string {
	rr := rec.RR()

	key := p.prefix + p.separator +
		strings.TrimSuffix(zone, ".") + p.separator +
		rr.Name + p.separator +
		rr.Type

	if rr.Type == "TXT" {
		key += p.separator + valueKey(rr.Data)
	}

	return key
}

// valueKey derives a short, stable key segment from a record's value. Hashing
// rather than using the value directly keeps arbitrary content, which may hold
// spaces or the configured separator, from breaking the key layout.
func valueKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

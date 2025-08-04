package etcd

import (
	"context"
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
	}

	return deleted, nil
}

func (p Plugin) fullName(zone string, rec libdns.Record) string {
	return p.prefix + p.separator +
		strings.TrimSuffix(zone, ".") + p.separator +
		rec.RR().Name + p.separator +
		rec.RR().Type
}

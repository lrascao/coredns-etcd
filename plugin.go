package etcd

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/miekg/dns"
)

const ttl = 604800

// Plugin is an etcd plugin
type Plugin struct {
	Next      plugin.Handler
	zone      string
	prefix    string
	separator string
	client    *etcd.Client
	// Guards record writes. A pointer because every method here takes Plugin
	// by value, and a sync.Mutex field would be copied along with it, leaving
	// each call locking its own private copy and excluding nothing.
	mu *sync.Mutex
}

// ServeDNS implements the plugin.Handler interface. This method gets called when etcd plugin is used
// in a Server.
func (p Plugin) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	req := request.Request{W: w, Req: r}

	qname := req.QName()

	// Debug log that we've have seen the query. This will only be shown when the debug plugin is loaded.
	log.Infof("class: %s, type: %s, qname: %s", req.Class(), req.Type(), qname)

	zone := plugin.Zones([]string{p.zone}).Matches(qname)
	if zone == "" {
		return plugin.NextOrFailure(p.Name(), p.Next, ctx, w, r)
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, true, true

	rnames, t, err := p.lookup(ctx, qname, zone)
	if err != nil {
		return plugin.NextOrFailure(p.Name(), p.Next, ctx, w, r)
	}

	switch t {
	case dns.Type(dns.TypeA):
		m.Answer = []dns.RR{
			&dns.A{
				Hdr: dns.RR_Header{
					Name:   qname,
					Ttl:    ttl,
					Class:  dns.ClassINET,
					Rrtype: dns.TypeA,
				},
				A: net.ParseIP(rnames[0]).To4(),
			},
		}
	case dns.Type(dns.TypeCNAME):
		rrnames, _, err := p.lookup(ctx, rnames[0], zone)
		if err != nil {
			return dns.RcodeServerFailure, errors.Wrap(err, "unable to lookup name on etcd")
		}

		m.Answer = []dns.RR{
			&dns.CNAME{
				Hdr: dns.RR_Header{
					Name:   qname,
					Ttl:    ttl,
					Class:  dns.ClassINET,
					Rrtype: dns.TypeCNAME,
				},
				Target: rnames[0],
			},
			&dns.A{
				Hdr: dns.RR_Header{
					Name:   rnames[0],
					Ttl:    ttl,
					Class:  dns.ClassINET,
					Rrtype: dns.TypeA,
				},
				A: net.ParseIP(rrnames[0]).To4(),
			},
		}
	case dns.Type(dns.TypeTXT):
		// One RR per value rather than a single RR holding several strings: a
		// resolver concatenates the strings within one TXT record, which would
		// corrupt an ACME challenge value.
		m.Answer = make([]dns.RR, 0, len(rnames))
		for _, v := range rnames {
			m.Answer = append(m.Answer,
				&dns.TXT{
					Hdr: dns.RR_Header{
						Name:   qname,
						Ttl:    ttl,
						Class:  dns.ClassINET,
						Rrtype: dns.TypeTXT,
					},
					Txt: []string{v},
				})
		}
	default:
		return dns.RcodeNotImplemented, fmt.Errorf("unsupported query type: %v", req.QType())
	}

	if len(m.Answer) > 0 {
		req.SizeAndDo(m)
		m = req.Scrub(m)

		log.Infof("answer: %v", m.Answer)

		w.WriteMsg(m)
	}

	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (p Plugin) Name() string { return "etcd" }

// Client exposes the underlying etcd client so that other plugins in the same
// server block can reuse this connection rather than opening a second one and
// duplicating its endpoint and TLS configuration.
func (p Plugin) Client() *etcd.Client { return p.client }

// recordType extracts the record type from what follows a name's key prefix.
// A and CNAME keys end at the type, so the remainder is the type itself. TXT
// keys carry a further per-value segment, so that one name can hold several of
// them, and the type is what precedes it.
func recordType(rest, separator string) string {
	tp, _, _ := strings.Cut(rest, separator)
	return tp
}

// lookup returns every value stored for qname, along with their shared record
// type. Only TXT ever yields more than one, which an ACME DNS-01 challenge for
// a domain and its wildcard requires: both authorizations are validated at the
// same _acme-challenge name and must be present at once.
func (p Plugin) lookup(ctx context.Context, qname, zone string) ([]string, dns.Type, error) {
	var t dns.Type

	// lowercase the qname
	qname = strings.ToLower(qname)

	name := strings.TrimSuffix(
		strings.TrimSuffix(qname, zone), ".")

	fullname := p.prefix + p.separator +
		strings.TrimSuffix(zone, ".") + p.separator +
		name + p.separator

	kvc := etcd.NewKV(p.client)

	res, err := kvc.Get(ctx, fullname, etcd.WithPrefix())
	if err != nil {
		return nil, t, errors.Wrap(err, "could not get DNS name")
	}

	log.Infof("full name: %s, # results: %v", fullname, len(res.Kvs))

	records := make([]record, 0, len(res.Kvs))
	for _, kv := range res.Kvs {
		k := string(kv.Key)
		v := string(kv.Value)

		log.Infof("%s (@ %s), key %s: %v", qname, fullname, k, v)

		records = append(records, record{key: k, value: v})
	}

	return selectRecords(records, fullname, p.separator, zone)
}

// record is a single key/value as stored in etcd.
type record struct {
	key   string
	value string
}

// selectRecords reduces the keys found under a name to the values to answer
// with and the type they share. It is separate from the etcd round trip purely
// so that it can be exercised directly, this being the path that serves every
// query the plugin handles.
func selectRecords(records []record, fullname, separator, zone string) ([]string, dns.Type, error) {
	var t dns.Type

	if len(records) == 0 {
		return nil, t, errors.New("unexpected number of records")
	}

	// Group by type, a name is still expected to hold exactly one of them.
	byType := make(map[string][]string, 1)
	for _, r := range records {
		tp := recordType(strings.TrimPrefix(r.key, fullname), separator)
		byType[tp] = append(byType[tp], r.value)
	}
	if len(byType) != 1 {
		return nil, t, errors.New("unexpected number of records")
	}

	var tp string
	var values []string
	for k, v := range byType {
		tp, values = k, v
	}

	// Keep answers stable across queries, map iteration is not ordered.
	sort.Strings(values)

	switch tp {
	case "A":
		t = dns.Type(dns.TypeA)
	case "CNAME":
		t = dns.Type(dns.TypeCNAME)
		for i := range values {
			values[i] = values[i] + "." + zone
		}
	case "TXT":
		t = dns.Type(dns.TypeTXT)
	default:
		return nil, t, fmt.Errorf("unsupported record type: %s", tp)
	}

	// A and CNAME are single valued, more than one is a broken zone rather
	// than something to answer with.
	if t != dns.Type(dns.TypeTXT) && len(values) != 1 {
		return nil, t, errors.New("unexpected number of records")
	}

	return values, t, nil
}

// ResponsePrinter wrap a dns.ResponseWriter and will write example to standard output when WriteMsg is called.
type ResponsePrinter struct {
	dns.ResponseWriter
}

// WriteMsg calls the underlying ResponseWriter's WriteMsg method and prints "example" to standard output.
func (r *ResponsePrinter) WriteMsg(res *dns.Msg) error {
	return r.ResponseWriter.WriteMsg(res)
}

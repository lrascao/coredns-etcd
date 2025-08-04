package etcd

import (
	"context"
	"fmt"
	"net"
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
	mu        sync.Mutex
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

	rname, t, err := p.lookup(ctx, qname, zone)
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
				A: net.ParseIP(rname).To4(),
			},
		}
	case dns.Type(dns.TypeCNAME):
		rrname, _, err := p.lookup(ctx, rname, zone)
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
				Target: rname,
			},
			&dns.A{
				Hdr: dns.RR_Header{
					Name:   rname,
					Ttl:    ttl,
					Class:  dns.ClassINET,
					Rrtype: dns.TypeA,
				},
				A: net.ParseIP(rrname).To4(),
			},
		}
	case dns.Type(dns.TypeTXT):
		m.Answer = []dns.RR{
			&dns.TXT{
				Hdr: dns.RR_Header{
					Name:   qname,
					Ttl:    ttl,
					Class:  dns.ClassINET,
					Rrtype: dns.TypeTXT,
				},
				Txt: []string{rname},
			},
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

func (p Plugin) lookup(ctx context.Context, qname, zone string) (string, dns.Type, error) {
	var t dns.Type

	name := strings.TrimSuffix(
		strings.TrimSuffix(qname, zone), ".")

	fullname := p.prefix + p.separator +
		strings.TrimSuffix(zone, ".") + p.separator +
		name + p.separator

	// lowercase the whole thing
	fullname = strings.ToLower(fullname)

	kvc := etcd.NewKV(p.client)

	res, err := kvc.Get(ctx, fullname, etcd.WithPrefix())
	if err != nil {
		return "", t, errors.Wrap(err, "could not get DNS name")
	}

	log.Infof("full name: %s, # results: %v", fullname, len(res.Kvs))

	// we're expecting only one result
	if len(res.Kvs) != 1 {
		return "", t, errors.New("unexpected number of records")
	}

	k := string(res.Kvs[0].Key)
	v := string(res.Kvs[0].Value)
	tp := strings.TrimPrefix(k, fullname)
	log.Infof("%s (@ %s), key %s: %v (type %s)", qname, fullname, k, v, tp)

	switch tp {
	case "A":
		t = dns.Type(dns.TypeA)
	case "CNAME":
		t = dns.Type(dns.TypeCNAME)
		v = v + "." + zone
	case "TXT":
		t = dns.Type(dns.TypeTXT)
		v = v
	default:
		return "", t, fmt.Errorf("unsupported record type: %s", tp)
	}

	return v, t, nil
}

// ResponsePrinter wrap a dns.ResponseWriter and will write example to standard output when WriteMsg is called.
type ResponsePrinter struct {
	dns.ResponseWriter
}

// WriteMsg calls the underlying ResponseWriter's WriteMsg method and prints "example" to standard output.
func (r *ResponsePrinter) WriteMsg(res *dns.Msg) error {
	return r.ResponseWriter.WriteMsg(res)
}

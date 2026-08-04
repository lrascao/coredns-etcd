package etcd

import (
	"context"
	"testing"

	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
)

// A query outside the configured zone must be handed straight to the next
// plugin, without an etcd round trip. This is the path every unrelated query in
// the server takes, so it has to hold even when the plugin has no client.
func TestServeDNSPassesOnQueriesOutsideTheZone(t *testing.T) {
	nextCalled := false
	next := test.HandlerFunc(func(_ context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
		nextCalled = true
		return dns.RcodeSuccess, nil
	})

	// No etcd client: reaching a lookup would panic, which is the point.
	p := Plugin{Next: next, zone: "test.domain.", prefix: "/coredns", separator: "/"}

	req := new(dns.Msg)
	req.SetQuestion("www.other.example.", dns.TypeA)

	rcode, err := p.ServeDNS(context.Background(), &test.ResponseWriter{}, req)
	if err != nil {
		t.Fatalf("ServeDNS() error = %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Errorf("rcode = %v, want %v", rcode, dns.RcodeSuccess)
	}
	if !nextCalled {
		t.Error("the next plugin was not called for a name outside the zone")
	}
}

// With no next plugin, an out-of-zone query is a server failure rather than a
// silent success.
func TestServeDNSWithoutANextPlugin(t *testing.T) {
	p := Plugin{zone: "test.domain.", prefix: "/coredns", separator: "/"}

	req := new(dns.Msg)
	req.SetQuestion("www.other.example.", dns.TypeA)

	rcode, err := p.ServeDNS(context.Background(), &test.ResponseWriter{}, req)
	if err == nil {
		t.Error("ServeDNS() error = nil, want an error when nothing follows in the chain")
	}
	if rcode != dns.RcodeServerFailure {
		t.Errorf("rcode = %v, want %v", rcode, dns.RcodeServerFailure)
	}
}

func TestResponsePrinterWritesThrough(t *testing.T) {
	inner := &test.ResponseWriter{}
	printer := &ResponsePrinter{ResponseWriter: inner}

	m := new(dns.Msg)
	m.SetQuestion("www.test.domain.", dns.TypeA)

	if err := printer.WriteMsg(m); err != nil {
		t.Errorf("WriteMsg() error = %v", err)
	}
}

package etcd

import (
	"strings"
	"testing"

	"github.com/libdns/libdns"
)

func testPlugin() Plugin {
	return Plugin{prefix: "/coredns", separator: "/"}
}

func txt(name, data string) libdns.Record {
	return libdns.RR{Name: name, Type: "TXT", Data: data}
}

// TXT keys carry a per-value segment so one name can hold several of them.
// Every other type keeps the single key per name it has always had, which is
// what keeps this change away from the records that serve the zone.
func TestFullName(t *testing.T) {
	p := testPlugin()

	tests := []struct {
		name string
		rec  libdns.Record
		want string
	}{
		{
			"A keeps its single key",
			libdns.RR{Name: "www", Type: "A", Data: "1.2.3.4"},
			"/coredns/test.domain/www/A",
		},
		{
			"CNAME keeps its single key",
			libdns.RR{Name: "alias", Type: "CNAME", Data: "www"},
			"/coredns/test.domain/alias/CNAME",
		},
		{
			"TXT gains a value segment",
			txt("_acme-challenge", "value-one"),
			"/coredns/test.domain/_acme-challenge/TXT/" + valueKey("value-one"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := p.fullName("test.domain.", test.rec); got != test.want {
				t.Errorf("fullName() = %q, want %q", got, test.want)
			}
		})
	}
}

// The two authorizations behind a domain-plus-wildcard order are validated at
// the same name, so their values must land on different keys.
func TestFullNameSeparatesTXTValues(t *testing.T) {
	p := testPlugin()

	one := p.fullName("test.domain.", txt("_acme-challenge", "value-one"))
	two := p.fullName("test.domain.", txt("_acme-challenge", "value-two"))

	if one == two {
		t.Fatalf("both values share the key %q, the second would overwrite the first", one)
	}

	prefix := "/coredns/test.domain/_acme-challenge/TXT/"
	for _, k := range []string{one, two} {
		if !strings.HasPrefix(k, prefix) {
			t.Errorf("key %q does not sit under %q, lookup would not find it", k, prefix)
		}
	}
}

// Re-presenting the same value has to overwrite its own key rather than pile
// up duplicates, since the value is the only thing identifying it.
func TestFullNameIsIdempotentForTheSameValue(t *testing.T) {
	p := testPlugin()

	one := p.fullName("test.domain.", txt("_acme-challenge", "same"))
	two := p.fullName("test.domain.", txt("_acme-challenge", "same"))

	if one != two {
		t.Errorf("same value produced %q and %q", one, two)
	}
}

// Values can hold spaces, equals signs and the separator itself, an SPF record
// being the obvious case. None of that may leak into the key.
func TestValueKeyIsSafeForAwkwardValues(t *testing.T) {
	for _, value := range []string{
		"v=spf1 +mx a:test.domain/28 ~all",
		"has/separator",
		"has spaces",
		"",
	} {
		got := valueKey(value)
		if got == "" {
			t.Errorf("valueKey(%q) is empty", value)
		}
		if strings.ContainsAny(got, "/ =") {
			t.Errorf("valueKey(%q) = %q, which contains a character unsafe for the key layout", value, got)
		}
	}
}

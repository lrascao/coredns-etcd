package etcd

import (
	"reflect"
	"testing"

	"github.com/miekg/dns"
)

const (
	testZone     = "test.domain."
	testFullname = "/coredns/test.domain/www/"
)

// recordType reads the type out of whatever follows a name's key prefix. A and
// CNAME keys end at the type; TXT keys carry a further value segment.
func TestRecordType(t *testing.T) {
	tests := []struct {
		name      string
		rest      string
		separator string
		want      string
	}{
		{"A", "A", "/", "A"},
		{"CNAME", "CNAME", "/", "CNAME"},
		{"TXT without a value segment, as older keys were written", "TXT", "/", "TXT"},
		{"TXT with a value segment", "TXT/9f86d081884c7d65", "/", "TXT"},
		{"a separator other than slash", "TXT.9f86d081884c7d65", ".", "TXT"},
		{"unknown type is passed through for the caller to reject", "SRV", "/", "SRV"},
		{"empty", "", "/", ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := recordType(test.rest, test.separator); got != test.want {
				t.Errorf("recordType(%q, %q) = %q, want %q", test.rest, test.separator, got, test.want)
			}
		})
	}
}

// Keys written before TXT values were separated must still resolve, so that
// existing zone data needs no migration.
func TestRecordTypeAcceptsPreExistingKeys(t *testing.T) {
	fullname := "/coredns/test.domain/_acme-challenge/"

	for _, key := range []string{
		fullname + "TXT",                      // written by an older version
		fullname + "TXT/" + valueKey("value"), // written by this one
	} {
		if got := recordType(key[len(fullname):], "/"); got != "TXT" {
			t.Errorf("recordType for key %q = %q, want TXT", key, got)
		}
	}
}

func TestSelectRecords(t *testing.T) {
	tests := []struct {
		name       string
		records    []record
		wantValues []string
		wantType   dns.Type
		wantErr    bool
	}{
		{
			name:       "a single A record",
			records:    []record{{testFullname + "A", "1.2.3.4"}},
			wantValues: []string{"1.2.3.4"},
			wantType:   dns.Type(dns.TypeA),
		},
		{
			name:       "a CNAME is qualified with the zone",
			records:    []record{{testFullname + "CNAME", "target"}},
			wantValues: []string{"target." + testZone},
			wantType:   dns.Type(dns.TypeCNAME),
		},
		{
			name:       "a single TXT record",
			records:    []record{{testFullname + "TXT/" + valueKey("hello"), "hello"}},
			wantValues: []string{"hello"},
			wantType:   dns.Type(dns.TypeTXT),
		},
		{
			name: "several TXT records at one name, as a wildcard order needs",
			records: []record{
				{testFullname + "TXT/" + valueKey("second"), "second"},
				{testFullname + "TXT/" + valueKey("first"), "first"},
			},
			wantValues: []string{"first", "second"},
			wantType:   dns.Type(dns.TypeTXT),
		},
		{
			name:       "a TXT key written before values were separated",
			records:    []record{{testFullname + "TXT", "legacy"}},
			wantValues: []string{"legacy"},
			wantType:   dns.Type(dns.TypeTXT),
		},
		{
			name:    "no records at all",
			records: nil,
			wantErr: true,
		},
		{
			name: "a name holding more than one type is ambiguous",
			records: []record{
				{testFullname + "A", "1.2.3.4"},
				{testFullname + "TXT/" + valueKey("hello"), "hello"},
			},
			wantErr: true,
		},
		{
			name: "two A records is a broken zone, not an answer",
			records: []record{
				{testFullname + "A", "1.2.3.4"},
				{testFullname + "A", "5.6.7.8"},
			},
			wantErr: true,
		},
		{
			name:    "an unsupported type is rejected",
			records: []record{{testFullname + "SRV", "0 0 443 target"}},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values, typ, err := selectRecords(test.records, testFullname, "/", testZone)

			if (err != nil) != test.wantErr {
				t.Fatalf("selectRecords() error = %v, wantErr %v", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if !reflect.DeepEqual(values, test.wantValues) {
				t.Errorf("values = %v, want %v", values, test.wantValues)
			}
			if typ != test.wantType {
				t.Errorf("type = %v, want %v", typ, test.wantType)
			}
		})
	}
}

// Answers must not depend on Go's map iteration order, which would otherwise
// make multi-value TXT responses shuffle between identical queries.
func TestSelectRecordsIsDeterministic(t *testing.T) {
	records := []record{
		{testFullname + "TXT/" + valueKey("c"), "c"},
		{testFullname + "TXT/" + valueKey("a"), "a"},
		{testFullname + "TXT/" + valueKey("b"), "b"},
	}

	want := []string{"a", "b", "c"}
	for i := 0; i < 20; i++ {
		got, _, err := selectRecords(records, testFullname, "/", testZone)
		if err != nil {
			t.Fatalf("selectRecords() error = %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: values = %v, want %v", i, got, want)
		}
	}
}

func TestName(t *testing.T) {
	if got := (Plugin{}).Name(); got != "etcd" {
		t.Errorf("Name() = %q, want %q", got, "etcd")
	}
}

func TestReady(t *testing.T) {
	if !(Plugin{}).Ready() {
		t.Error("Ready() = false, want true")
	}
}

func TestClientExposesTheUnderlyingConnection(t *testing.T) {
	if got := (Plugin{}).Client(); got != nil {
		t.Errorf("Client() = %v, want nil for a zero Plugin", got)
	}
}

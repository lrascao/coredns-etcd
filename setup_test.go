package etcd

import (
	"reflect"
	"testing"

	"github.com/coredns/caddy"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		shouldErr bool
		want      config
	}{
		{
			name: "a full block",
			input: `etcd {
				endpoints https://192.168.3.42:2379
				ca /etc/coredns/ca-etcd.pem
				cert /etc/coredns/cert-etcd.pem
				key /etc/coredns/cert-etcd-key.pem
				prefix /coredns
				separator /
			}`,
			want: config{
				endpoints: []string{"https://192.168.3.42:2379"},
				caFile:    "/etc/coredns/ca-etcd.pem",
				certFile:  "/etc/coredns/cert-etcd.pem",
				keyFile:   "/etc/coredns/cert-etcd-key.pem",
				prefix:    "/coredns",
				separator: "/",
			},
		},
		{
			name: "several endpoints on one line",
			input: `etcd {
				endpoints https://192.168.3.42:2379 https://192.168.3.43:2379 https://192.168.3.44:2379
				prefix /coredns
				separator /
			}`,
			want: config{
				endpoints: []string{
					"https://192.168.3.42:2379",
					"https://192.168.3.43:2379",
					"https://192.168.3.44:2379",
				},
				prefix:    "/coredns",
				separator: "/",
			},
		},
		{
			name: "a separator other than slash",
			input: `etcd {
				endpoints https://192.168.3.42:2379
				prefix coredns
				separator .
			}`,
			want: config{
				endpoints: []string{"https://192.168.3.42:2379"},
				prefix:    "coredns",
				separator: ".",
			},
		},
		{
			name:      "no block at all",
			input:     `etcd`,
			shouldErr: true,
		},
		{
			name: "prefix without a value",
			input: `etcd {
				prefix
			}`,
			shouldErr: true,
		},
		{
			name: "ca without a value",
			input: `etcd {
				ca
			}`,
			shouldErr: true,
		},
		{
			name: "cert without a value",
			input: `etcd {
				cert
			}`,
			shouldErr: true,
		},
		{
			name: "key without a value",
			input: `etcd {
				key
			}`,
			shouldErr: true,
		},
		{
			name: "separator without a value",
			input: `etcd {
				separator
			}`,
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := caddy.NewTestController("dns", test.input)

			got, err := parseConfig(c)
			if (err != nil) != test.shouldErr {
				t.Fatalf("parseConfig() error = %v, shouldErr %v", err, test.shouldErr)
			}
			if test.shouldErr {
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("parseConfig() = %+v, want %+v", got, test.want)
			}
		})
	}
}

// A block that opens and closes immediately is rejected the same way a missing
// one is, since caddy reports both as no block at all.
func TestParseConfigRejectsAnEmptyBlock(t *testing.T) {
	c := caddy.NewTestController("dns", "etcd {\n}")

	if _, err := parseConfig(c); err == nil {
		t.Error("parseConfig() error = nil, want an error for an empty block")
	}
}

// Unknown keys are ignored rather than rejected, so a typo silently loses its
// setting. Documented here because it is a trap worth being explicit about.
func TestParseConfigIgnoresUnknownKeys(t *testing.T) {
	c := caddy.NewTestController("dns", `etcd {
		endpoints https://192.168.3.42:2379
		prefx /coredns
	}`)

	got, err := parseConfig(c)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got.prefix != "" {
		t.Errorf("prefix = %q, want it empty since the key was misspelled", got.prefix)
	}
}

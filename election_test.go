package etcd

import (
	"context"
	"errors"
	"testing"
)

func TestElectionOptions(t *testing.T) {
	var cfg electionConfig

	// The key is used exactly as given, the caller owns its placement.
	WithElection("/coredns/acme/election/acme-dns01")(&cfg)
	if want := "/coredns/acme/election/acme-dns01"; cfg.prefix != want {
		t.Errorf("prefix = %q, want %q", cfg.prefix, want)
	}

	WithProposal("node1")(&cfg)
	if cfg.proposal != "node1" {
		t.Errorf("proposal = %q, want %q", cfg.proposal, "node1")
	}

	called := false
	WithCallback(func(context.Context) error {
		called = true
		return nil
	})(&cfg)
	if cfg.cb == nil {
		t.Fatal("callback was not set")
	}
	if err := cfg.cb(context.Background()); err != nil {
		t.Errorf("callback returned %v", err)
	}
	if !called {
		t.Error("the callback that was stored is not the one that was passed")
	}
}

// Options are applied in order, so a later one wins. Worth pinning down since
// callers build the list dynamically.
func TestElectionOptionsLastWins(t *testing.T) {
	var cfg electionConfig

	for _, setter := range []ElectionOption{
		WithElection("first"),
		WithElection("second"),
	} {
		setter(&cfg)
	}

	if want := "second"; cfg.prefix != want {
		t.Errorf("prefix = %q, want %q", cfg.prefix, want)
	}
}

// Now that the key is taken verbatim, an empty one is indistinguishable from
// never having called WithElection, and Campaign rejects both.
func TestCampaignRejectsAnEmptyElectionKey(t *testing.T) {
	err := Plugin{}.Campaign(context.Background(),
		WithElection(""), WithProposal("node1"))

	if !errors.Is(err, ErrPrefixNotSet) {
		t.Errorf("Campaign() error = %v, want %v", err, ErrPrefixNotSet)
	}
}

// Campaign validates before it touches etcd, so these paths are reachable
// without a connection, and they are the ones a misconfigured caller hits.
func TestCampaignRejectsIncompleteConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		setters []ElectionOption
		want    error
	}{
		{
			"no options at all",
			nil,
			ErrPrefixNotSet,
		},
		{
			"a proposal without an election name",
			[]ElectionOption{WithProposal("node1")},
			ErrPrefixNotSet,
		},
		{
			"an election name without a proposal",
			[]ElectionOption{WithElection("acme-dns01")},
			ErrProposalNotSet,
		},
		{
			"an empty proposal is treated as missing",
			[]ElectionOption{WithElection("acme-dns01"), WithProposal("")},
			ErrProposalNotSet,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := Plugin{}.Campaign(context.Background(), test.setters...)
			if !errors.Is(err, test.want) {
				t.Errorf("Campaign() error = %v, want %v", err, test.want)
			}
		})
	}
}

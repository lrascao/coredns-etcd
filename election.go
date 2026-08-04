package etcd

import (
	"context"
	"errors"
	"fmt"

	"go.etcd.io/etcd/client/v3/concurrency"
)

type Election interface {
	Campaign(context.Context, ...ElectionOption) error
}

type Callback func(ctx context.Context) error

type electionConfig struct {
	prefix   string
	proposal string
	cb       Callback
}

type ElectionOption func(*electionConfig)

var (
	ErrPrefixNotSet   = errors.New("election prefix must be set")
	ErrProposalNotSet = errors.New("election proposal must be set")
)

// WithElection sets the etcd key the election is held under. The key is used
// as given: callers own their own keyspace, so that election state can sit
// alongside whatever else they keep in etcd rather than in a namespace this
// package picks for them.
func WithElection(key string) ElectionOption {
	return func(e *electionConfig) {
		e.prefix = key
	}
}

func WithProposal(proposal string) ElectionOption {
	return func(e *electionConfig) {
		e.proposal = proposal
	}
}

func WithCallback(cb Callback) ElectionOption {
	return func(e *electionConfig) {
		e.cb = cb
	}
}

func (p Plugin) Campaign(ctx context.Context, setters ...ElectionOption) error {
	var cfg electionConfig
	for _, setter := range setters {
		setter(&cfg)
	}

	if cfg.prefix == "" {
		return ErrPrefixNotSet
	}
	if cfg.proposal == "" {
		return ErrProposalNotSet
	}

	session, err := concurrency.NewSession(p.client)
	if err != nil {
		return fmt.Errorf("error creating etcd concurrency session: %w", err)
	}

	e := concurrency.NewElection(session, cfg.prefix)

	if err = e.Campaign(ctx, cfg.proposal); err != nil {
		return fmt.Errorf("error campaigning for election: %w", err)
	}

	// invoke leader callback since election was won
	if cfg.cb != nil {
		log.Debugf("election won, executing callback (key: %s)", e.Key())
		if err := cfg.cb(ctx); err != nil {
			return fmt.Errorf("error executing callback after winning election: %w", err)
		}
	}

	select {
	case <-ctx.Done():
	case <-session.Done():
		return errors.New("elect: session expired")
	}

	if err := e.Resign(ctx); err != nil {
		return fmt.Errorf("error resigning from election: %w", err)
	}

	return nil
}

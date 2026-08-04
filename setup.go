package etcd

import (
	"context"
	"fmt"
	"sync"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
)

// Define log to be a logger with the plugin name in it. This way we can just use log.Info and
// friends to log.
var log = clog.NewWithPlugin("etcd")

// init registers this plugin.
func init() {
	plugin.Register("etcd", setup)
}

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
// config is everything the etcd block carries.
type config struct {
	endpoints []string
	caFile    string
	certFile  string
	keyFile   string
	prefix    string
	separator string
}

// parseConfig reads the etcd block. Kept apart from setup so that it can be
// tested without opening a connection.
func parseConfig(c *caddy.Controller) (config, error) {
	var cfg config

	c.Next() // Ignore "etcd" and give us the next token.

	if !c.NextBlock() { // Expects a block
		return config{}, fmt.Errorf("no block found: %w", c.ArgErr()) // Otherwise it's an error.
	}

	for {
		value := c.Val() // Use the value.
		switch value {
		case "endpoints":
			for c.NextArg() {
				cfg.endpoints = append(cfg.endpoints, c.Val())
			}
		case "ca":
			if !c.NextArg() {
				return config{}, c.ArgErr()
			}
			cfg.caFile = c.Val()
		case "cert":
			if !c.NextArg() {
				return config{}, c.ArgErr()
			}
			cfg.certFile = c.Val()
		case "key":
			if !c.NextArg() {
				return config{}, c.ArgErr()
			}
			cfg.keyFile = c.Val()
		case "prefix":
			if !c.NextArg() {
				return config{}, c.ArgErr()
			}
			cfg.prefix = c.Val()
		case "separator":
			if !c.NextArg() {
				return config{}, c.ArgErr()
			}
			cfg.separator = c.Val()
		}
		if !c.Next() {
			break
		}
	}

	return cfg, nil
}

// setup is the function that gets called when the config parser see the token "example". Setup is responsible
// for parsing any extra options the example plugin may have. The first token this function sees is "example".
func setup(c *caddy.Controller) error {
	conf, err := parseConfig(c)
	if err != nil {
		return err
	}

	prefix, separator := conf.prefix, conf.separator

	client, err := NewClient(context.Background(),
		conf.endpoints, conf.caFile, conf.certFile, conf.keyFile)
	if err != nil {
		return fmt.Errorf("unable to create etcd client: %w", err)
	}

	// Shared by every Plugin value built below, so that they all serialize
	// record writes against one another rather than each against itself.
	mu := new(sync.Mutex)

	// Add the Plugin to CoreDNS, so Servers can use it in their plugin chain.
	cfg := dnsserver.GetConfig(c)
	cfg.AddPlugin(
		func(next plugin.Handler) plugin.Handler {
			return Plugin{
				Next:      next,
				zone:      cfg.Zone,
				prefix:    prefix,
				separator: separator,
				client:    client,
				mu:        mu,
			}
		})

	// All OK, return a nil error.
	return nil
}

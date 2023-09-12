package etcd

import (
	"context"
	"fmt"

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
func setup(c *caddy.Controller) error {
	c.Next() // Ignore "etcd" and give us the next token.

	if !c.NextBlock() { // Expects a block
		return fmt.Errorf("no block found: %w", c.ArgErr()) // Otherwise it's an error.
	}

	var (
		endpoints                 []string
		caFile, certFile, keyFile string
		prefix                    string
		separator                 string
	)
	for {
		value := c.Val() // Use the value.
		switch value {
		case "endpoints":
			for c.NextArg() {
				endpoints = append(endpoints, c.Val())
			}
		case "ca":
			if !c.NextArg() {
				return c.ArgErr()
			}
			caFile = c.Val()
		case "cert":
			if !c.NextArg() {
				return c.ArgErr()
			}
			certFile = c.Val()
		case "key":
			if !c.NextArg() {
				return c.ArgErr()
			}
			keyFile = c.Val()
		case "prefix":
			if !c.NextArg() {
				return c.ArgErr()
			}
			prefix = c.Val()
		case "separator":
			if !c.NextArg() {
				return c.ArgErr()
			}
			separator = c.Val()
		}
		if !c.Next() {
			break
		}
	}

	client, err := NewClient(context.Background(), endpoints, caFile, certFile, keyFile)
	if err != nil {
		return fmt.Errorf("unable to create etcd client: %w", err)
	}

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
			}
		})

	// All OK, return a nil error.
	return nil
}

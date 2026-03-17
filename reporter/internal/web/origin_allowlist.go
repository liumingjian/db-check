package web

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// originAllowlist matches HTTP Origin header values against configuration.
//
// It supports:
// - Full origins (scheme://host[:port]), e.g. http://localhost:3000
// - Host-only entries (host or host:port), e.g. localhost or localhost:3000
// - Wildcard "*" meaning allow all origins (for local dev; dangerous)
type originAllowlist struct {
	allowAll bool

	// origins is a strict allowlist of normalized origins (scheme://host[:port]).
	origins map[string]struct{}

	// hosts is a host allowlist (hostname only, any port, any scheme).
	hosts map[string]struct{}

	// hostPorts is a host:port allowlist (any scheme).
	hostPorts map[string]struct{}

	// wsPatterns is the set of host patterns for websocket.AcceptOptions.OriginPatterns.
	wsPatterns []string
}

var localInterfaceHostsCache struct {
	once  sync.Once
	hosts []string
}

func newOriginAllowlist(allowed []string) originAllowlist {
	out := originAllowlist{
		origins:   make(map[string]struct{}),
		hosts:     make(map[string]struct{}),
		hostPorts: make(map[string]struct{}),
	}

	wsSeen := make(map[string]struct{}, len(allowed))
	addWSPattern := func(pattern string) {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "" {
			return
		}
		if _, ok := wsSeen[pattern]; ok {
			return
		}
		wsSeen[pattern] = struct{}{}
		out.wsPatterns = append(out.wsPatterns, pattern)
	}

	addHostPort := func(host, port string) {
		h := strings.ToLower(strings.TrimSpace(host))
		p := strings.TrimSpace(port)
		if h == "" || p == "" {
			return
		}
		out.hostPorts[net.JoinHostPort(h, p)] = struct{}{}
		addWSPattern(net.JoinHostPort(h, p))
	}

	// When users whitelist localhost/loopback (common for local dev), allow the same port
	// on other local interface addresses too (e.g. 192.168.x.x). This avoids surprising
	// CORS failures when the frontend is opened via the dev server "Network" address.
	expandLocalPort := func(host, hostPort string) {
		if !isLoopbackHost(host) || strings.TrimSpace(hostPort) == "" {
			return
		}
		_, port, err := net.SplitHostPort(hostPort)
		if err != nil || strings.TrimSpace(port) == "" {
			return
		}
		for _, ip := range localInterfaceHosts() {
			addHostPort(ip, port)
		}
	}

	for _, raw := range allowed {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if s == "*" {
			out.allowAll = true
			continue
		}

		// Full origin entry.
		if strings.Contains(s, "://") {
			key, host, hostPort, ok := normalizeOriginKey(s)
			if !ok {
				continue
			}
			out.origins[key] = struct{}{}
			wsHost := wsHostFromOrigin(s)
			addWSPattern(wsHost)
			expandLocalPort(host, wsHost)
			expandLocalPort(host, hostPort)
			continue
		}

		// Host-only entry.
		host, hostPort, ok := normalizeHostEntry(s)
		if !ok {
			continue
		}
		if hostPort != "" {
			addWSPattern(hostPort)
			out.hostPorts[hostPort] = struct{}{}
			expandLocalPort(host, hostPort)
			continue
		}
		addWSPattern(host)
		addWSPattern(host + ":*")
		out.hosts[host] = struct{}{}
	}

	return out
}

func localInterfaceHosts() []string {
	localInterfaceHostsCache.once.Do(func() {
		seen := make(map[string]struct{})
		addIP := func(ip net.IP) {
			if ip == nil {
				return
			}
			if v4 := ip.To4(); v4 != nil {
				ip = v4
			}
			if ip.IsUnspecified() {
				return
			}
			// Keep the expansion conservative: only loopback/private addresses.
			if !(ip.IsLoopback() || ip.IsPrivate()) {
				return
			}
			s := strings.ToLower(strings.TrimSpace(ip.String()))
			if s == "" {
				return
			}
			if _, ok := seen[s]; ok {
				return
			}
			seen[s] = struct{}{}
			localInterfaceHostsCache.hosts = append(localInterfaceHostsCache.hosts, s)
		}

		// Ensure loopback is always present.
		addIP(net.ParseIP("127.0.0.1"))
		addIP(net.ParseIP("::1"))

		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				addIP(v.IP)
			case *net.IPAddr:
				addIP(v.IP)
			}
		}
	})
	return localInterfaceHostsCache.hosts
}

func isLoopbackHost(host string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return false
	}
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

func wsHostFromOrigin(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimRight(s, "/")
	if s == "" || s == "*" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(u.Host))
	if host == "" {
		return ""
	}
	p := strings.TrimSpace(u.Port())
	if p != "" && isDefaultPort(u.Scheme, p) {
		hn := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if hn == "" {
			return ""
		}
		host = bracketIPv6(hn)
	}
	return host
}

func (a originAllowlist) allows(originHeader string) bool {
	if a.allowAll {
		return true
	}
	key, host, hostPort, ok := normalizeOriginKey(originHeader)
	if !ok {
		return false
	}
	if _, ok := a.origins[key]; ok {
		return true
	}
	if hostPort != "" {
		if _, ok := a.hostPorts[hostPort]; ok {
			return true
		}
	}
	if host != "" {
		if _, ok := a.hosts[host]; ok {
			return true
		}
	}
	return false
}

func normalizeHostEntry(raw string) (host string, hostPort string, ok bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimRight(s, "/")
	if s == "" || strings.ContainsAny(s, "/?#") {
		return "", "", false
	}

	// url.Parse needs a scheme to treat this as a host.
	u, err := url.Parse("http://" + s)
	if err != nil {
		return "", "", false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if h == "" {
		return "", "", false
	}
	p := strings.TrimSpace(u.Port())
	if p == "" {
		return h, "", true
	}
	if _, err := strconv.Atoi(p); err != nil {
		return "", "", false
	}
	return h, net.JoinHostPort(h, p), true
}

func normalizeOriginKey(raw string) (originKey string, host string, hostPort string, ok bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimRight(s, "/")
	if s == "" {
		return "", "", "", false
	}

	// Special-case "null" origin. It has no host/port.
	if s == "null" {
		return "null", "", "", true
	}

	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", false
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	if scheme == "" {
		return "", "", "", false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if h == "" {
		return "", "", "", false
	}
	p := strings.TrimSpace(u.Port())
	if p != "" {
		if _, err := strconv.Atoi(p); err != nil {
			return "", "", "", false
		}
		if isDefaultPort(scheme, p) {
			p = ""
		}
	}

	if p == "" {
		originKey = scheme + "://" + bracketIPv6(h)
	} else {
		originKey = scheme + "://" + net.JoinHostPort(h, p)
	}

	// hostPort is used for host:port allowlists. It must include a port.
	effectivePort := p
	if effectivePort == "" {
		if dp, ok := defaultPort(scheme); ok {
			effectivePort = dp
		}
	}
	if effectivePort != "" {
		hostPort = net.JoinHostPort(h, effectivePort)
	}

	return originKey, h, hostPort, true
}

func isDefaultPort(scheme, port string) bool {
	dp, ok := defaultPort(scheme)
	return ok && dp == port
}

func defaultPort(scheme string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "http":
		return "80", true
	case "https":
		return "443", true
	default:
		return "", false
	}
}

func bracketIPv6(host string) string {
	// net.JoinHostPort will do this for host:port, but for scheme://host without a port
	// we need to add brackets ourselves.
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") && !strings.HasSuffix(host, "]") {
		return "[" + host + "]"
	}
	return host
}

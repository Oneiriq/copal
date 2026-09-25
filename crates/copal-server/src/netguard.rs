//! Outbound-destination policy for server-initiated requests.
//!
//! Webhook endpoints are tenant-supplied URLs that the dispatcher
//! fetches from inside the deployment, which is the classic
//! server-side request forgery shape: without a policy, a tenant can
//! aim deliveries at loopback, private ranges, or a cloud metadata
//! endpoint and read the response codes as an oracle. Every candidate
//! host resolves here and every resolved address must be public.
//!
//! The guard runs at registration AND again at delivery, because DNS
//! answers change between the two; the delivery client also refuses
//! redirects, since following one would reach an unchecked address.
//! URL ingestion follows redirects by hand instead, vetting each hop
//! and pinning its connection to the addresses that passed.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs as _};

use copal_core::CopalError;

/// Whether an address is reachable-but-forbidden: anything not
/// globally routable, plus the cloud metadata address specifically.
fn is_forbidden(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            // The link-local metadata service, called out because it
            // is the one address whose contents are always sensitive.
            *v4 == Ipv4Addr::new(169, 254, 169, 254)
                || v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                // Carrier-grade NAT and benchmarking ranges.
                || matches!(v4.octets(), [100, 64..=127, _, _] | [198, 18..=19, _, _])
                // Reserved 240/4.
                || v4.octets()[0] >= 240
        }
        IpAddr::V6(v6) => {
            *v6 == Ipv6Addr::LOCALHOST
                || v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // Unique local (fc00::/7) and link-local (fe80::/10).
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // IPv4-mapped addresses inherit the v4 verdict.
                || v6.to_ipv4_mapped().is_some_and(|v4| is_forbidden(&IpAddr::V4(v4)))
        }
    }
}

/// A destination that passed the policy: the URL as the HTTP client
/// will read it, and, for a named host, the addresses that were
/// checked.
#[derive(Debug, Clone)]
pub struct Vetted {
    pub url: reqwest::Url,
    /// `(host, addresses)` for a named host, `None` for an IP literal.
    /// A client resolving the name again can get a different answer
    /// (DNS rebinding), so a caller that pins its connection to these
    /// addresses connects to exactly what was checked.
    pub pin: Option<(String, Vec<SocketAddr>)>,
}

/// Parse an outbound URL with the parser the HTTP client uses. A
/// guard that splits the string itself disagrees with the client on
/// inputs such as `http://10.0.0.1\@example.com/`, which WHATWG parsing
/// sends to 10.0.0.1: the guard would have checked one host and the
/// client connected to another.
fn parse_outbound(raw: &str) -> copal_core::Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw)
        .map_err(|_| CopalError::validation("target_url is not a valid URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(CopalError::validation("target_url must be http or https"));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(CopalError::validation("target_url has no host"));
    }
    Ok(url)
}

/// Accept an outbound URL for a request the policy is switched off for:
/// still http(s) with a host, but no address check and nothing pinned.
pub fn accept_outbound_url(raw: &str) -> copal_core::Result<Vetted> {
    Ok(Vetted {
        url: parse_outbound(raw)?,
        pin: None,
    })
}

/// Check that a URL is http(s), names a host, and that every address
/// the host resolves to is public, returning what was checked.
/// Resolution failure refuses: an unresolvable destination cannot be
/// delivered to anyway.
pub fn vet_outbound_url(raw: &str) -> copal_core::Result<Vetted> {
    let refuse = |reason: &str| CopalError::validation(format!("target_url {reason}"));

    let url = parse_outbound(raw)?;
    let port = url.port_or_known_default().unwrap_or(80);
    let (resolved, pin_host): (Vec<SocketAddr>, Option<String>) = match url.domain() {
        Some(domain) => (
            (domain, port)
                .to_socket_addrs()
                .map_err(|_| refuse("does not resolve"))?
                .collect(),
            Some(domain.to_owned()),
        ),
        // An IP literal (the parser has already normalized forms like
        // `2130706433`); IPv6 arrives bracketed.
        None => {
            let literal = url.host_str().unwrap_or_default();
            let ip: IpAddr = literal
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .map_err(|_| refuse("has no host"))?;
            (vec![SocketAddr::new(ip, port)], None)
        }
    };
    if resolved.is_empty() {
        return Err(refuse("does not resolve"));
    }
    // Every answer must be public: one private address in a round-robin
    // set is enough to reach an internal service.
    if resolved.iter().any(|addr| is_forbidden(&addr.ip())) {
        return Err(refuse("resolves to a non-public address"));
    }
    Ok(Vetted {
        url,
        pin: pin_host.map(|host| (host, resolved)),
    })
}

/// [`vet_outbound_url`] for callers that only need the verdict.
pub fn check_outbound_url(raw: &str) -> copal_core::Result<()> {
    vet_outbound_url(raw).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_and_metadata_addresses_are_forbidden() {
        for raw in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.0.5",
            "172.16.9.9",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(is_forbidden(&ip), "{raw} must be forbidden");
        }
        for raw in ["1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"] {
            let ip: IpAddr = raw.parse().unwrap();
            assert!(!is_forbidden(&ip), "{raw} must be allowed");
        }
    }

    #[test]
    fn urls_are_parsed_before_resolution() {
        assert!(check_outbound_url("ftp://example.com/hook").is_err());
        assert!(check_outbound_url("https://").is_err());
        assert!(check_outbound_url("http://127.0.0.1:9000/hook").is_err());
        assert!(check_outbound_url("http://user:pass@127.0.0.1/hook").is_err());
        assert!(check_outbound_url("http://[::1]:8080/hook").is_err());
        assert!(check_outbound_url("http://localhost/hook").is_err());
    }

    #[test]
    fn the_guard_checks_the_host_the_client_connects_to() {
        // WHATWG parsing ends the authority at the backslash, so the
        // client connects to the address before it. A hand split on
        // '@' checked the public literal after it instead.
        assert!(check_outbound_url("http://127.0.0.1\\@1.1.1.1/").is_err());
        assert!(check_outbound_url("http://169.254.169.254\\@1.1.1.1/").is_err());
        let vetted = vet_outbound_url("http://1.1.1.1\\@127.0.0.1/").unwrap();
        assert_eq!(vetted.url.host_str(), Some("1.1.1.1"));
        assert!(vetted.pin.is_none(), "an IP literal resolves nothing");

        // Numeric and hex host forms normalize to the loopback address.
        assert!(check_outbound_url("http://2130706433/").is_err());
        assert!(check_outbound_url("http://0x7f.0.0.1/").is_err());
    }

    #[test]
    fn accepting_resolves_nothing_and_vetting_resolves_names() {
        // With the policy off nothing is resolved or pinned; with it on,
        // a name is judged by what it resolves to.
        let accepted = accept_outbound_url("http://localhost:8080/x").unwrap();
        assert!(accepted.pin.is_none());
        let err = vet_outbound_url("http://localhost:8080/x").unwrap_err();
        assert!(err.to_string().contains("non-public"), "{err}");
        assert!(accept_outbound_url("ftp://example.com/").is_err());
    }
}

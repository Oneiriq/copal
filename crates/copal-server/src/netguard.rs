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

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs as _};

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

/// Check that a URL is http(s), names a host, and that every address
/// the host resolves to is public. Resolution failure refuses: an
/// unresolvable destination cannot be delivered to anyway.
pub fn check_outbound_url(raw: &str) -> copal_core::Result<()> {
    let refuse = |reason: &str| CopalError::validation(format!("target_url {reason}"));

    let rest = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .ok_or_else(|| refuse("must be http or https"))?;
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| refuse("has no host"))?;
    // Strip userinfo; credentials in a webhook URL are a smell and the
    // host is what follows the last '@' regardless.
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = match authority.rsplit_once(':') {
        // Bracketed IPv6 keeps its colons; only a trailing numeric
        // port splits.
        Some((h, p)) if !h.ends_with(']') && p.chars().all(|c| c.is_ascii_digit()) => {
            (h, p.parse::<u16>().unwrap_or(443))
        }
        _ => (
            authority,
            if raw.starts_with("https://") { 443 } else { 80 },
        ),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() {
        return Err(refuse("has no host"));
    }

    let resolved: Vec<IpAddr> = (host, port)
        .to_socket_addrs()
        .map_err(|_| refuse("does not resolve"))?
        .map(|addr| addr.ip())
        .collect();
    if resolved.is_empty() {
        return Err(refuse("does not resolve"));
    }
    // Every answer must be public: one private address in a round-robin
    // set is enough to reach an internal service.
    if resolved.iter().any(is_forbidden) {
        return Err(refuse("resolves to a non-public address"));
    }
    Ok(())
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
}

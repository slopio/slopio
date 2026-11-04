use super::*;

fn uri(s: &str) -> Uri {
    s.parse().unwrap_or_else(|e| panic!("{s:?}: {e}"))
}

#[test]
fn origin_form() {
    let u = uri("/search?q=io_uring");
    assert_eq!(u.path(), "/search");
    assert_eq!(u.query(), Some("q=io_uring"));
    assert!(u.scheme().is_none() && u.authority().is_none());
    assert_eq!(u, "/search?q=io_uring");
}

#[test]
fn absolute_form() {
    let u = uri("HTTPS://user@Example.com:8443/a/b?c#frag");
    assert_eq!(u.scheme(), Some(&Scheme::HTTPS));
    assert_eq!(u.authority().unwrap(), "user@example.com:8443");
    assert_eq!(u.host(), Some("Example.com"));
    assert_eq!(u.port_u16(), Some(8443));
    assert_eq!(u.path(), "/a/b");
    assert_eq!(u.query(), Some("c"));
    assert_eq!(u.to_string(), "https://user@Example.com:8443/a/b?c");
}

#[test]
fn absolute_without_path_gets_a_slash() {
    let u = uri("http://example.com");
    assert_eq!(u.path(), "/");
    assert_eq!(u, "http://example.com/");
}

#[test]
fn authority_form_and_star() {
    let u = uri("example.com:443");
    assert_eq!(u.path(), "");
    assert_eq!(u.port_u16(), Some(443));
    assert_eq!(uri("*").path(), "*");
}

#[test]
fn other_schemes() {
    let u = uri("grpc+tls://svc:50051/pkg.Svc/Call");
    assert_eq!(u.scheme_str(), Some("grpc+tls"));
    assert_eq!(u.path(), "/pkg.Svc/Call");
}

#[test]
fn ipv6_hosts() {
    let u = uri("http://[::1]:8080/");
    assert_eq!(u.host(), Some("[::1]"));
    assert_eq!(u.port_u16(), Some(8080));
    assert_eq!(uri("http://[::1]/").port(), None);
    assert!(Uri::try_from("http://[::1/").is_err());
    assert!(Uri::try_from("http://::1]/").is_err());
}

#[test]
fn rejects_bad_authorities() {
    for bad in [
        "http://a:1:2/",
        "http://user@/",
        "http://ho%st/",
        "http:///path",
        "http://a b/",
    ] {
        assert!(Uri::try_from(bad).is_err(), "{bad:?}");
    }
    assert!(
        Authority::try_from("u%40ser@host").is_ok(),
        "percent in userinfo"
    );
}

#[test]
fn rejects_bad_paths() {
    for bad in ["/a b", "/a\r\n", "/\0", "/a?b c"] {
        assert!(Uri::try_from(bad).is_err(), "{bad:?}");
    }
    assert!(Uri::try_from(&b"/caf\xc3\xa9"[..]).is_ok());
    assert!(Uri::try_from(&b"/caf\xc3"[..]).is_err(), "not UTF-8");
    assert!(Uri::try_from("").is_err());
}

#[test]
fn shared_parts_point_into_the_input() {
    let src = Bytes::copy_from_slice(b"https://example.com/index.html");
    let at = src.as_ptr() as usize;
    let u = Uri::from_shared(src).unwrap();
    assert_eq!(u.authority().unwrap().as_str().as_ptr() as usize, at + 8);
    assert_eq!(u.path().as_ptr() as usize, at + 19);
}

#[test]
fn from_parts_rules() {
    let parts = Parts {
        scheme: Some(Scheme::HTTP),
        authority: None,
        path_and_query: Some(PathAndQuery::from_static("/")),
    };
    assert!(Uri::from_parts(parts).is_err());
    let parts = Parts {
        scheme: None,
        authority: Some(Authority::from_static("a.com")),
        path_and_query: Some(PathAndQuery::from_static("/")),
    };
    assert!(Uri::from_parts(parts).is_err());
}

#[test]
fn builder() {
    let u = Uri::builder()
        .scheme("https")
        .authority("example.com")
        .path_and_query("/x?y=1")
        .build()
        .unwrap();
    assert_eq!(u, "https://example.com/x?y=1");
    assert!(Uri::builder().scheme("https").build().is_err());
    assert!(Uri::builder().path_and_query("no-slash").build().is_err());
}

#[test]
fn round_trip_through_parts() {
    let u = uri("http://a.com/p?q");
    let parts = u.clone().into_parts();
    assert_eq!(Uri::from_parts(parts).unwrap(), u);
}

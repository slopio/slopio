//! The ways popple-h2 and popple-http2 use the `http` crate, against this one.

use slopio::http::header::{self, HeaderName, HeaderValue};
use slopio::http::uri::Scheme;
use slopio::http::{HeaderMap, Method, Request, Response, StatusCode, Uri, Version};

const GRPC: HeaderValue = HeaderValue::from_static("application/grpc");

#[test]
fn constants_work_as_patterns() {
    let kind = |name: &HeaderName| match *name {
        header::CONTENT_LENGTH => 1,
        header::CONNECTION | header::TE | header::UPGRADE => 2,
        _ => 0,
    };
    assert_eq!(
        kind(&HeaderName::from_lowercase(b"content-length").unwrap()),
        1
    );
    assert_eq!(kind(&HeaderName::from_lowercase(b"te").unwrap()), 2);
    assert_eq!(kind(&HeaderName::from_lowercase(b"x-other").unwrap()), 0);

    let safe = |m: &Method| matches!(*m, Method::GET | Method::HEAD);
    assert!(safe(&Method::from_bytes(b"HEAD").unwrap()));
}

fn method_or_get(method: Option<&Method>) -> &Method {
    method.unwrap_or(&Method::GET)
}

#[test]
fn a_constant_method_borrows_as_static() {
    assert_eq!(method_or_get(None), Method::GET);
    assert_eq!(method_or_get(Some(&Method::PUT)), Method::PUT);
}

#[test]
fn header_map_as_the_grpc_code_uses_it() {
    let mut fields = HeaderMap::with_capacity(2);
    fields.insert("grpc-status", HeaderValue::from(13usize));
    fields.insert(header::CONTENT_TYPE, GRPC);
    fields.append(&header::CONTENT_TYPE, GRPC);
    assert!(
        fields
            .try_append(
                HeaderName::from_static("grpc-message"),
                HeaderValue::from_static("x")
            )
            .is_ok()
    );
    assert_eq!(fields.get("grpc-status").unwrap(), "13");
    assert!(fields.contains_key(&header::CONTENT_TYPE));
    for value in fields.get_all(header::CONTENT_TYPE) {
        assert_eq!(value.as_bytes(), b"application/grpc");
    }
    let mut seen = 0;
    for (name, value) in &fields {
        seen += 1;
        assert!(!name.as_str().is_empty() && !value.is_empty());
    }
    assert_eq!(seen, 4);
    let taken = std::cell::RefCell::new(fields.clone()).take();
    assert_eq!(taken, fields);
}

#[test]
fn requests_and_responses_as_the_client_builds_them() {
    let uri: Uri = "https://api.example.com:8443/pkg.Svc/Call".parse().unwrap();
    let parts = uri.clone().into_parts();
    assert_eq!(parts.scheme, Some(Scheme::HTTPS));
    let uri = Uri::builder()
        .scheme(parts.scheme.unwrap())
        .authority(parts.authority.unwrap())
        .path_and_query("/pkg.Svc/Other")
        .build()
        .unwrap();
    assert_eq!(uri.authority().unwrap().host(), "api.example.com");
    assert_eq!(uri.authority().unwrap().port_u16(), Some(8443));

    let mut req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("te", "trailers")
        .body(())
        .unwrap();
    req.headers_mut().insert(header::CONTENT_TYPE, GRPC);
    assert_eq!(req.method(), Method::POST);
    assert_eq!(req.uri().scheme_str(), Some("https"));
    assert_eq!(
        req.uri().path_and_query().unwrap().as_str(),
        "/pkg.Svc/Other"
    );

    let mut res = Response::new(());
    *res.status_mut() = StatusCode::from_bytes(b"200").unwrap();
    *res.version_mut() = Version::HTTP_2;
    *res.headers_mut() = req.headers().clone();
    assert!(res.status().is_success());
    assert_eq!(u16::from(res.status()), 200);
    assert_eq!(res.into_parts().0.headers.len(), 2);
}

use std::{cell::UnsafeCell, rc::Rc};

use crate::quic::{ConnectionState, Http3Request, HttpRequest, HttpResponse, process_request};

pub async fn handle_request(
    slab_id: usize,
    stream_id: u64,
    conn_state: Rc<UnsafeCell<ConnectionState>>,
) {
    let Some(request) = Http3Request::new(slab_id).await else {
        process_request::remove(slab_id);
        return;
    };
    process_request::remove(slab_id);
    let response = route(&request).await;
    let state = unsafe { &mut *conn_state.get() };
    state.send_response(stream_id, response);
}

async fn route(request: &HttpRequest) -> HttpResponse {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => HttpResponse::new(200)
            .header("content-type", "text/plain")
            .body("Hello from HTTP/3!"),
        _ => HttpResponse::new(404)
            .header("content-type", "text/plain")
            .body("Not Found"),
    }
}

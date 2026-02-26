use crate::{
    quic::{
        BodyKind, HttpHandler, HttpResponse, RequestHead,
        stream::{WaitForBody, take_meta},
    },
    tracing::new_trace,
};

use super::{
    connection::{ConnectionId, StreamId},
    server::with_state,
};

pub(super) fn task_done(connection_id: ConnectionId) {
    with_state(|state| {
        let Some(h3) = state.handler.connections.get_mut(&connection_id) else {
            return;
        };
        h3.in_flight -= 1;
        if !h3.alive && h3.in_flight == 0 {
            state.handler.connections.remove(&connection_id);
        }
    });
}

pub(super) fn send_response(
    connection_id: ConnectionId,
    stream_id: StreamId,
    response: HttpResponse,
) {
    with_state(|state| {
        let Some(endpoint) = &mut state.endpoint else {
            return;
        };
        let Some(h3) = state.handler.connections.get_mut(&connection_id) else {
            return;
        };
        let Some(conn) = endpoint.conn_get_mut(connection_id) else {
            return;
        };
        h3.send_response(conn, stream_id, response);
        if let Err(e) = endpoint.process_connections() {
            eprintln!("process_connections after send_response: {e}");
        }
    });
}

pub(super) async fn handle_request(
    slab_id: usize,
    stream_id: StreamId,
    connection_id: ConnectionId,
) {
    let _span = new_trace("http3 request");

    let Some(meta) = take_meta(slab_id) else {
        task_done(connection_id);
        return;
    };

    let handler = with_state(|state| {
        state.request_handler.as_ref().map(|h| unsafe {
            // SAFETY: HttpHandler is 'static, the reference is valid for the duration of this call.
            &*(h.as_ref() as *const dyn HttpHandler)
        })
    });
    let Some(handler) = handler else {
        task_done(connection_id);
        return;
    };

    let head = RequestHead::from(meta);

    if let Err(response) = handler.on_headers(&head).await {
        send_response(connection_id, stream_id, response);
        task_done(connection_id);
        return;
    }

    let response = match head.body_kind {
        BodyKind::None => handler.no_body(head).await,
        BodyKind::Buffered { .. } => match (WaitForBody { id: slab_id }).await {
            Ok(Some(body)) => handler.buffered_body(head, body).await,
            _ => {
                task_done(connection_id);
                return;
            }
        },
        BodyKind::Streamed => unimplemented!(),
    };

    send_response(connection_id, stream_id, response);
    #[cfg(feature = "test-monitor")]
    super::monitor::record_request_served();
    task_done(connection_id);
}

use bytes::Bytes;
use http::Response;
use runtime::runtime_h2::server;
use runtime::runtime_h2::workflow::{self, Body, ConnectionId, RecvState};

fn main() {
    runtime::main_thread(async {
        println!("h2 server listening on :8080");

        workflow::serve(8080, |conn_id, mut state, respond| async move {
            handle_request(conn_id, &mut state, respond).await;
        })
        .await
        .unwrap();
    });
}

async fn handle_request(
    _conn_id: ConnectionId,
    state: &mut RecvState,
    mut respond: server::SendResponse<Bytes>,
) {
    let body = state.recv_all().await.unwrap();

    match body {
        Body::Empty { head } => {
            println!("none");
            let payload = Bytes::from_static(b"Hello from h2 runtime!\n");
            let response = Response::builder()
                .status(200)
                .header("content-type", "text/plain")
                .header("content-length", payload.len())
                .body(())
                .unwrap();
            let mut send = respond.send_response(response, false).unwrap();
            send.reserve_capacity(payload.len());
            send.send_data(payload, true).unwrap();
        }
        Body::Complete { head, bytes } => {
            let bbb = bytes.to_vec();
            let bb = String::from_utf8(bbb).unwrap();
            println!("complete body: {bb}");
            let response = Response::builder().status(200).body(()).unwrap();
            respond.send_response(response, true).unwrap();
        }
        Body::Stream { head, recv } => {
            let response = Response::builder().status(501).body(()).unwrap();
            respond.send_response(response, true).unwrap();
        }
    }
}

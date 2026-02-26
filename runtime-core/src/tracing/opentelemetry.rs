use opentelemetry_sdk::trace::SpanData;

use crate::{
    channels::{Sender, unbounded_channel},
    runtime::spawn,
    tracing::collector::Collector,
};

pub struct TraceSink {
    sender: Sender<SpanData>,
}

impl TraceSink {
    pub fn push_span(&self, trace: SpanData) {
        self.sender.send(trace);
    }
}

pub fn init_trace_sink<P: Collector + 'static>() -> TraceSink {
    let (sender, recv) = unbounded_channel();
    spawn(async move {
        let mut processor = P::init();
        while let Some(batch) = recv.recv_all().await {
            processor.process(batch).await;
        }
    });
    TraceSink { sender }
}

use crate::{
    channels::{Sender, unbounded_channel},
    runtime::spawn,
    tracing::{collector::Collector, types::SpanData},
};

pub struct TraceSink {
    sender: Sender<SpanData>,
}

impl TraceSink {
    pub fn push_span(&self, span: SpanData) {
        self.sender.send(span);
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

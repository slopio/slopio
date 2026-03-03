use crate::{
    channels::{Sender, unbounded_channel},
    runtime::spawn,
    tracing::{collector::Collector, span::SpanWrapper},
};

pub struct TraceSink {
    sender: Sender<SpanWrapper>,
}

impl TraceSink {
    pub fn push_span(&self, buf: SpanWrapper) {
        self.sender.send(buf);
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

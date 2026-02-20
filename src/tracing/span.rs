use rand::Rng;

pub struct SpanMetadata {
    pub(crate) trace_id: u128,
    pub(crate) span_id: u64,
    pub(crate) parent_span_id: Option<u64>,
    pub(crate) name: &'static str,
    pub(crate) events: Vec<Event>,
}

pub(crate) struct Event {}

impl SpanMetadata {
    pub fn new(trace_id: u128, parent_span_id: Option<u64>, name: &'static str) -> Self {
        Self {
            trace_id,
            span_id: rand::thread_rng().r#gen(),
            parent_span_id,
            name,
            events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: Event) {}
}

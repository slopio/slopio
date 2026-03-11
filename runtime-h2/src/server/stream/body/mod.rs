#[derive(Debug)]
pub enum Body {
    Chunked,
    Entire { size: usize },
}

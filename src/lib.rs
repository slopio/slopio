pub mod io_driver;
mod macros;
pub mod runtime;
mod task;

pub fn block_on<F: Future>(f: F) -> F::Output {
    let mut rt = runtime::RuntimeInner::default();
    rt.block_on(f)
}

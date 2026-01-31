# A faire
- new traits for OPs needing a buffer
  {IO_uring communication for shutdown?
  {communication through ring
- numastat monitoring
- timeout on socket operations
- (???) thread communication (64 align)

- macros to generalize select/join for n Futures.
- better shutdown (how can it be graceful ?)


align io buffers 64 for vectorization

BUFFERMORE


mimaloc : 
MIMALLOC_LARGE_OS_PAGES=1 \
MIMALLOC_PAGE_RESET=0 \
MIMALLOC_EAGER_COMMIT=1 \


cancels for different recv and buffer recyling if cancelled when cqe comes.

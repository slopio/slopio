# A faire
- new traits for OPs needing a buffer
  {IO_uring communication for shutdown?
  {communication through ring
- numastat monitoring
- timeout on socket operations
- (???) thread communication (64 align)

- macros to generalize select/join for n Futures.
- better shutdown (how can it be graceful ?)

constant time in the event loop -> needs to improve the time driver.

align io buffers 64 for vectorization

BUFFERMORE


test http3 with : 
/snap/bin/curl --http3 -k https://localhost:4433/{1,2,3} --parallel



mimaloc : 
MIMALLOC_LARGE_OS_PAGES=1 \
MIMALLOC_PAGE_RESET=0 \
MIMALLOC_EAGER_COMMIT=1 \


cancels for different recv and buffer recyling if cancelled when cqe comes.

H2 :
optimization :
- add metadata inside the data frame vec to avoid allocating for nothing

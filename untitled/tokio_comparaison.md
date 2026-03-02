We want to compare tokio and my runtime on a TCP benchmark,
I want you to write a simple app in single core (tokio main thread / main thread for my runtime) to handle TCP ping pong.
when a connection is accepted, you spawn a task to handle it.

do this for both and I'll review.

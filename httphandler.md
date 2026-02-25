In my HTTP3 integration, whenever a new stream is created, I spawn() a new task.
for the moment is task is waiting for the request to be totally available.
I want the handling to be done in two steps.

# Intention
I want to be able to run an async handler on the headers before the body, and then based on what body it is, I want to be able to run different handlers. I also want to be able to reject the request before getting the body.

# Headers/metadata Handling
I want first want the handler to be able to know about the method, the headers etc, but not getting the body.
at this point we will know what the type of body is (if there is one).

# Body Handling
based on the type of body, I want different handlers :
- when there is no body (can run immediatly)
- a handler for the case where the body needs to be received in integrality
- a handler for streamed body
-

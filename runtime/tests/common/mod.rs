use std::sync::{Arc, Condvar, Mutex};

pub struct Cv {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl Clone for Cv {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Cv {
    pub fn new() -> (Self, Self) {
        let cv = Cv {
            inner: Arc::new((Mutex::new(false), Condvar::new())),
        };
        (cv.clone(), cv)
    }

    pub fn notify(&self) {
        let mut b = self.inner.0.lock().unwrap();
        *b = true;
        self.inner.1.notify_one();
    }

    pub fn wait(&self) {
        let mut k = self.inner.0.lock().unwrap();
        while !*k {
            k = self.inner.1.wait(k).unwrap();
        }
    }
}

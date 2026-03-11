use crate::server::stream::StreamError;

#[derive(Debug)]
pub struct SendWindow(i64);

impl SendWindow {
    pub fn get(&self) -> i64 {
        self.0
    }

    pub fn init(value: u32) -> Self {
        Self(value as i64)
    }

    pub fn add(&mut self, increment: u32) -> Result<(), StreamError> {
        match self.0.checked_add(increment as i64) {
            Some(v) if v <= 0x7FFF_FFFF => {
                self.0 = v;
                Ok(())
            }
            _ => Err(StreamError::WindowOverflow),
        }
    }

    pub fn consume(&mut self, amount: i64) {
        self.0 -= amount;
    }

    /// Adjust window when SETTINGS_INITIAL_WINDOW_SIZE changes.
    pub fn adjust_initial(&mut self, old_initial: u32, new_initial: u32) -> Result<(), StreamError> {
        let delta = new_initial as i64 - old_initial as i64;
        match self.0.checked_add(delta) {
            Some(v) if v <= 0x7FFF_FFFF => {
                self.0 = v;
                Ok(())
            }
            _ => Err(StreamError::WindowOverflow),
        }
    }
}

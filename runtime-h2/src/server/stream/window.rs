#[derive(Debug)]
pub struct SendWindow(i64);

impl SendWindow {
    pub fn get(&self) -> i64 {
        self.0
    }

    pub fn init(value: u32) -> Self {
        Self(value as i64)
    }

    pub fn add_ok(&mut self, increment: u32) -> bool {
        match self.0.checked_add(increment as i64) {
            Some(v) if v <= 0x7FFF_FFFF => {
                self.0 = v;
                true
            }
            _ => false,
        }
    }

    pub fn consume(&mut self, amount: i64) {
        self.0 -= amount;
    }

    /// Adjust window when SETTINGS_INITIAL_WINDOW_SIZE changes.
    /// Returns false if the new window would exceed 2^31-1 (FLOW_CONTROL_ERROR).
    pub fn adjust_initial(&mut self, old_initial: u32, new_initial: u32) -> bool {
        let delta = new_initial as i64 - old_initial as i64;
        match self.0.checked_add(delta) {
            Some(v) if v <= 0x7FFF_FFFF => {
                self.0 = v;
                true
            }
            _ => false,
        }
    }
}

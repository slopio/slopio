use std::sync::OnceLock;

/// Resource attributes describing this service/process. Set once before starting the runtime.
pub struct ResourceConfig {
    pub service_name: &'static str,
    pub service_version: &'static str,
    /// Hostname of the machine (e.g. `tracing::hostname()`).
    pub host_name: Option<String>,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            service_name: "my-runtime",
            service_version: "",
            host_name: None,
        }
    }
}

static RESOURCE: OnceLock<ResourceConfig> = OnceLock::new();

/// Set the resource attributes for this process. Call before `main_thread` / `thread_per_core`.
/// Has no effect if called more than once.
pub fn set_resource(config: ResourceConfig) {
    let _ = RESOURCE.set(config);
}

pub(crate) fn resource_config() -> &'static ResourceConfig {
    RESOURCE.get_or_init(ResourceConfig::default)
}

/// Read the machine hostname from `/etc/hostname`.
pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

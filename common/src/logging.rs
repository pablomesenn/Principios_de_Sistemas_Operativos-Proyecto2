// common/src/logging.rs
// Logging estructurado con niveles y formato JSON opcional

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

impl LogLevel {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "DEBUG" => LogLevel::Debug,
            "WARN" | "WARNING" => LogLevel::Warn,
            "ERROR" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: LogLevel,
    pub component: String,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl LogEntry {
    pub fn new(level: LogLevel, component: &str, message: &str) -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            level,
            component: component.to_string(),
            message: message.to_string(),
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, key: &str, value: impl ToString) -> Self {
        self.fields.push((key.to_string(), value.to_string()));
        self
    }

    pub fn to_json(&self) -> String {
        let fields_json: Vec<String> = self.fields
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", k, v.replace('\"', "\\\"")))
            .collect();

        let fields_str = if fields_json.is_empty() {
            String::new()
        } else {
            format!(",{}", fields_json.join(","))
        };

        format!(
            "{{\"ts\":{},\"level\":\"{}\",\"component\":\"{}\",\"msg\":\"{}\"{}}}",
            self.timestamp,
            self.level,
            self.component,
            self.message.replace('\"', "\\\""),
            fields_str
        )
    }

    pub fn to_text(&self) -> String {
        let fields_str: String = self.fields
            .iter()
            .map(|(k, v)| format!(" {}={}", k, v))
            .collect();

        format!(
            "[{}] [{}] {}{}",
            self.level,
            self.component,
            self.message,
            fields_str
        )
    }
}

/// Logger con nivel mínimo configurable
pub struct Logger {
    component: String,
    min_level: LogLevel,
    json_format: bool,
}

impl Logger {
    pub fn new(component: &str) -> Self {
        let min_level = std::env::var("LOG_LEVEL")
            .map(|s| LogLevel::from_str(&s))
            .unwrap_or(LogLevel::Info);

        let json_format = std::env::var("LOG_FORMAT")
            .map(|s| s.to_lowercase() == "json")
            .unwrap_or(false);

        Self {
            component: component.to_string(),
            min_level,
            json_format,
        }
    }

    pub fn with_level(mut self, level: LogLevel) -> Self {
        self.min_level = level;
        self
    }

    pub fn with_json(mut self, json: bool) -> Self {
        self.json_format = json;
        self
    }

    fn log(&self, entry: LogEntry) {
        if entry.level >= self.min_level {
            if self.json_format {
                println!("{}", entry.to_json());
            } else {
                println!("{}", entry.to_text());
            }
        }
    }

    pub fn debug(&self, message: &str) -> LogEntry {
        let entry = LogEntry::new(LogLevel::Debug, &self.component, message);
        entry
    }

    pub fn info(&self, message: &str) -> LogEntry {
        let entry = LogEntry::new(LogLevel::Info, &self.component, message);
        entry
    }

    pub fn warn(&self, message: &str) -> LogEntry {
        let entry = LogEntry::new(LogLevel::Warn, &self.component, message);
        entry
    }

    pub fn error(&self, message: &str) -> LogEntry {
        let entry = LogEntry::new(LogLevel::Error, &self.component, message);
        entry
    }

    pub fn emit(&self, entry: LogEntry) {
        self.log(entry);
    }
}

// Macros para logging más ergonómico
#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $msg:expr) => {
        $logger.emit($logger.debug($msg))
    };
    ($logger:expr, $msg:expr, $($key:expr => $val:expr),*) => {
        $logger.emit($logger.debug($msg)$(.field($key, $val))*)
    };
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $msg:expr) => {
        $logger.emit($logger.info($msg))
    };
    ($logger:expr, $msg:expr, $($key:expr => $val:expr),*) => {
        $logger.emit($logger.info($msg)$(.field($key, $val))*)
    };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $msg:expr) => {
        $logger.emit($logger.warn($msg))
    };
    ($logger:expr, $msg:expr, $($key:expr => $val:expr),*) => {
        $logger.emit($logger.warn($msg)$(.field($key, $val))*)
    };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $msg:expr) => {
        $logger.emit($logger.error($msg))
    };
    ($logger:expr, $msg:expr, $($key:expr => $val:expr),*) => {
        $logger.emit($logger.error($msg)$(.field($key, $val))*)
    };
}
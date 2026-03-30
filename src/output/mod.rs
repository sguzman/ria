use std::fmt;
use std::io::{self, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
    Raw,
}

impl OutputFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "human" => Some(Self::Human),
            "json" => Some(Self::Json),
            "raw" => Some(Self::Raw),
            _ => None,
        }
    }
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Human => "human",
            Self::Json => "json",
            Self::Raw => "raw",
        };
        write!(f, "{value}")
    }
}

#[derive(Debug, Clone)]
pub struct OutputPolicy {
    pub format: OutputFormat,
    pub paging: bool,
    pub color: bool,
    pub quiet: bool,
    pub verbose: bool,
}

impl OutputPolicy {
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            paging: false,
            color: true,
            quiet: false,
            verbose: false,
        }
    }
}

pub struct OutputWriter {
    policy: OutputPolicy,
}

impl OutputWriter {
    pub fn new(policy: OutputPolicy) -> Self {
        Self { policy }
    }

    pub fn write_line(&self, line: &str) -> io::Result<()> {
        if self.policy.quiet {
            return Ok(());
        }
        let mut stdout = io::stdout();
        writeln!(stdout, "{line}")
    }

    pub fn write_error(&self, line: &str) -> io::Result<()> {
        let mut stderr = io::stderr();
        writeln!(stderr, "{line}")
    }
}

#[cfg(test)]
mod tests {
    use super::{OutputFormat, OutputPolicy, OutputWriter};

    #[test]
    fn parses_output_formats() {
        assert_eq!(OutputFormat::parse("human"), Some(OutputFormat::Human));
        assert_eq!(OutputFormat::parse("JSON"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::parse("raw"), Some(OutputFormat::Raw));
        assert_eq!(OutputFormat::parse("unknown"), None);
    }

    #[test]
    fn output_policy_defaults() {
        let policy = OutputPolicy::new(OutputFormat::Human);
        assert!(policy.color);
        assert!(!policy.quiet);
    }

    #[test]
    fn output_writer_respects_quiet() {
        let mut policy = OutputPolicy::new(OutputFormat::Human);
        policy.quiet = true;
        let writer = OutputWriter::new(policy);
        let result = writer.write_line("ignored");
        assert!(result.is_ok());
    }
}

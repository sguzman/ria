use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::OutputFormat;

    #[test]
    fn parses_output_formats() {
        assert_eq!(OutputFormat::parse("human"), Some(OutputFormat::Human));
        assert_eq!(OutputFormat::parse("JSON"), Some(OutputFormat::Json));
        assert_eq!(OutputFormat::parse("raw"), Some(OutputFormat::Raw));
        assert_eq!(OutputFormat::parse("unknown"), None);
    }
}

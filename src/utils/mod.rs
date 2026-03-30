use globset::{Glob, GlobSet, GlobSetBuilder};
use std::io::{self, IsTerminal, Read};

use crate::errors::{Error, Result};

pub struct GlobMatcher {
    set: GlobSet,
}

impl GlobMatcher {
    pub fn new(patterns: &[String]) -> Result<Self> {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            let glob = Glob::new(pattern)
                .map_err(|err| Error::message(format!("invalid glob '{pattern}': {err}")))?;
            builder.add(glob);
        }
        let set = builder
            .build()
            .map_err(|err| Error::message(format!("glob build error: {err}")))?;
        Ok(Self { set })
    }

    pub fn is_match(&self, candidate: &str) -> bool {
        self.set.is_match(candidate)
    }
}

pub fn validate_identifier(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    value.chars().all(|ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' )
    })
}

pub fn stdin_is_terminal() -> bool {
    io::stdin().is_terminal()
}

pub fn read_stdin() -> Result<String> {
    let mut buffer = String::new();
    let mut stdin = io::stdin();
    stdin.read_to_string(&mut buffer)?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_identifiers() {
        assert!(validate_identifier("item-123"));
        assert!(validate_identifier("item_123"));
        assert!(validate_identifier("item.123"));
        assert!(!validate_identifier(""));
        assert!(!validate_identifier("item 123"));
    }

    #[test]
    fn glob_matching() {
        let patterns = vec!["*.mp3".to_string(), "data/*.json".to_string()];
        let matcher = GlobMatcher::new(&patterns).expect("glob matcher");
        assert!(matcher.is_match("song.mp3"));
        assert!(matcher.is_match("data/test.json"));
        assert!(!matcher.is_match("image.png"));
    }

    #[test]
    fn stdin_terminal_check() {
        let _ = stdin_is_terminal();
    }
}

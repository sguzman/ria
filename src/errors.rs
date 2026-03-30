use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("missing command")]
    MissingCommand,
    #[error("{0}")]
    Message(String),
}

impl Error {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

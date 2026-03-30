#[derive(Debug, Default)]
pub struct HttpClient {
    pub user_agent: Option<String>,
}

impl HttpClient {
    pub fn new() -> Self {
        Self::default()
    }
}

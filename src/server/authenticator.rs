use crate::server::error::FForchaServerError;
use http::Request;
use http::header::AUTHORIZATION;
use log::warn;
use std::sync::Arc;

#[derive(Clone)]
pub struct FForchaServerAuthenticator {
    shared_secret: Arc<String>,
}

impl FForchaServerAuthenticator {
    pub fn new(shared_secret: String) -> Self {
        Self {
            shared_secret: Arc::new(shared_secret),
        }
    }

    pub fn authenticate(&self, request: Request<()>) -> Result<(), FForchaServerError> {
        match request.headers().get(AUTHORIZATION) {
            Some(header_value) => match header_value.to_str() {
                Ok(header_value)
                    if header_value == format!("Bearer {}", self.shared_secret).as_str() =>
                {
                    Ok(())
                }
                Ok(header_value) => {
                    warn!("Invalid authorization header: {header_value}");
                    Err(FForchaServerError::UnauthorizedWorker(request))
                }
                Err(error) => {
                    warn!("Failed to convert header value to string: {:?}", error);
                    Err(FForchaServerError::UnauthorizedWorker(request))
                }
            },
            None => {
                warn!("No authorization header found");
                Err(FForchaServerError::UnauthorizedWorker(request))
            }
        }
    }
}

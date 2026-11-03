//! Controlled operation failures. Preserve causes without exposing them as JSON.
use std::{borrow::Cow, error::Error};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Category {
    InvalidInput,
    Blocked,
    Inconclusive,
    Uncertain,
    AcknowledgedUnverified,
    Unavailable,
}

impl Category {
    pub fn code(self) -> u8 {
        match self {
            Self::InvalidInput => 2,
            Self::Blocked => 3,
            Self::Inconclusive => 4,
            Self::Uncertain => 5,
            Self::AcknowledgedUnverified => 6,
            Self::Unavailable => 7,
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            2 => Self::InvalidInput,
            3 => Self::Blocked,
            4 => Self::Inconclusive,
            5 => Self::Uncertain,
            6 => Self::AcknowledgedUnverified,
            _ => Self::Unavailable,
        }
    }
}

#[derive(Debug)]
pub struct Failure {
    pub category: Category,
    pub message: Cow<'static, str>,
    pub reason: Option<&'static str>,
    pub bootstrap_unavailable: bool,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl Failure {
    pub fn code(&self) -> u8 {
        self.category.code()
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)?;
        if let Some(cause) = &self.cause {
            write!(formatter, ": {cause}")?;
        }
        Ok(())
    }
}

impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn Error + 'static))
    }
}

pub fn fault(code: u8, message: impl Into<Cow<'static, str>>) -> Box<dyn Error + Send + Sync> {
    Box::new(Failure {
        category: Category::from_code(code),
        message: message.into(),
        reason: None,
        bootstrap_unavailable: false,
        cause: None,
    })
}

pub fn coded(
    code: u8,
    reason: &'static str,
    message: &'static str,
) -> Box<dyn Error + Send + Sync> {
    Box::new(Failure {
        category: Category::from_code(code),
        message: message.into(),
        reason: Some(reason),
        bootstrap_unavailable: false,
        cause: None,
    })
}

pub fn wrap(
    code: u8,
    message: &'static str,
    cause: Box<dyn Error + Send + Sync>,
) -> Box<dyn Error + Send + Sync> {
    Box::new(Failure {
        category: Category::from_code(code),
        message: message.into(),
        reason: None,
        bootstrap_unavailable: false,
        cause: Some(cause),
    })
}

pub fn bootstrap_unavailable() -> Box<dyn Error + Send + Sync> {
    Box::new(Failure {
        category: Category::Unavailable,
        message: "Kafka connection unavailable".into(),
        reason: None,
        bootstrap_unavailable: true,
        cause: None,
    })
}

pub fn status(code: u8) -> &'static str {
    match code {
        0 => "succeeded",
        2 => "invalid_input",
        3 => "blocked",
        4 => "inconclusive",
        5 => "uncertain",
        6 => "acknowledged_unverified",
        _ => "unavailable",
    }
}

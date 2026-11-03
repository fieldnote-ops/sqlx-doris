//! Explicit maintenance protocols; observations never authorize takeover.
pub mod diagnose;
pub mod drain;
pub mod identity;
use crate::{context::Context, failure::Result};

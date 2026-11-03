//! Per-operation evidence. No process output, global state or retry policy.
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Phase {
    #[default]
    Read,
    Local,
    Submitted,
    Acknowledged,
    Verified,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Self::Read => "read_only",
            Self::Local => "local_write",
            Self::Submitted => "submitted",
            Self::Acknowledged => "acknowledged",
            Self::Verified => "verified",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub phase: Phase,
    pub step: String,
    pub details: Value,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            phase: Phase::Read,
            step: "validation".into(),
            details: json!({}),
        }
    }
}

/// Typed progress and durable local breadcrumbs; rendering belongs to the caller.
pub enum Event {
    ReceiptPersisted { name: String, path: String },
    CredentialsPersisted { name: String, path: String },
    WorkObserved { entry: Value },
}

type Listener = Arc<dyn Fn(&Event) -> crate::failure::Result<()> + Send + Sync>;

/// Clones share one operation's evidence, including after a future is cancelled.
#[derive(Clone, Default)]
pub struct Observation(Arc<Mutex<Snapshot>>, Option<Listener>);

impl Observation {
    pub fn with_listener(
        mut self,
        listener: impl Fn(&Event) -> crate::failure::Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.1 = Some(Arc::new(listener));
        self
    }

    pub fn event(&self, event: Event) -> crate::failure::Result<()> {
        if let Some(listener) = &self.1 {
            listener(&event)?;
        }
        Ok(())
    }

    pub fn failure_reason(&self, reason: &str, message: &str) {
        let mut state = self.0.lock().unwrap();
        if state.details.get("issues").is_none() {
            state.details["issues"] = json!([{"code": reason, "message": message}]);
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.0.lock().unwrap().clone()
    }

    pub fn phase(&self, phase: Phase, step: &str) {
        let mut state = self.0.lock().unwrap();
        state.phase = phase;
        state.step = step.into();
    }

    pub fn data(&self, key: &str, value: Value) {
        self.0.lock().unwrap().details[key] = value;
    }

    pub fn observed_work(&self) -> Value {
        self.0
            .lock()
            .unwrap()
            .details
            .get("work")
            .cloned()
            .unwrap_or_else(|| json!({}))
    }

    pub fn work(&self, key: &str, value: Value) -> crate::failure::Result<()> {
        let mut state = self.0.lock().unwrap();
        if !state.details["work"].is_object() {
            state.details["work"] = json!({});
        }
        state.details["work"][key] = value.clone();
        drop(state);
        self.event(Event::WorkObserved { entry: value })
    }

    pub fn details(&self, value: Value) {
        self.0
            .lock()
            .unwrap()
            .details
            .as_object_mut()
            .unwrap()
            .extend(value.as_object().unwrap().clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_an_in_flight_future_preserves_submission_evidence() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let observation = Observation::default();
        let in_flight = observation.clone();
        let mut operation = Box::pin(async move {
            in_flight.phase(Phase::Submitted, "stop_routine");
            std::future::pending::<()>().await;
            in_flight.phase(Phase::Verified, "stop_verified");
        });
        assert_eq!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        );
        drop(operation);
        assert_eq!(observation.snapshot().phase, Phase::Submitted);
        assert_eq!(observation.snapshot().step, "stop_routine");
    }

    #[test]
    fn independent_calls_do_not_share_partial_work_or_mutation_phases() {
        let first = Observation::default();
        let nested = first.clone();
        let second = Observation::default();
        nested.phase(Phase::Submitted, "stop_routine");
        nested
            .work("transaction:7", json!({"blocked": true}))
            .unwrap();
        assert_eq!(first.snapshot().phase, Phase::Submitted);
        assert_eq!(first.observed_work()["transaction:7"]["blocked"], true);
        assert_eq!(second.snapshot().phase, Phase::Read);
        assert_eq!(second.observed_work(), json!({}));
    }
}

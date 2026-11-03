//! Read-only FE transaction observations; unknown never means never submitted.
use super::{Error, Result, StreamLoadClient};
use serde::Deserialize;

/// Current state reported for a label in the selected database.
/// Neither label reuse nor the identity/content of its original payload is attested.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelState {
    /// Missing, expired or otherwise unknown transaction history.
    Unknown,
    /// The transaction is being prepared.
    Prepare,
    /// Two-phase preparation completed; a coordinator still controls its outcome.
    Precommitted,
    /// Committed, but publication may still be pending.
    Committed,
    /// The transaction is published and visible.
    Visible,
    /// The server reports this transaction aborted; application reconciliation is separate.
    Aborted,
    /// A future or unrecognized server state; do not infer completion.
    Other(String),
}

/// One non-atomic, read-only observation through the explicitly configured FE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelObservation {
    /// Requested database; not proof of an unchanged database generation.
    pub database: String,
    /// Requested label; not proof of batch payload identity.
    pub label: String,
    /// Exact recognized state, or the unrecognized server text.
    pub state: LabelState,
}

#[derive(Deserialize)]
struct Reply {
    code: i64,
    data: String,
}

impl StreamLoadClient {
    /// Observe `/api/<db>/get_load_state?label=...` once, without retries or redirects.
    /// Valid credentials/read access do not attest write authorization. UNKNOWN,
    /// missing history and observation failures never justify replay or a new label.
    /// No commit, abort, offset change or business statement is submitted.
    pub async fn observe_label(&self, database: &str, label: &str) -> Result<LabelObservation> {
        if database.is_empty()
            || label.is_empty()
            || database.contains('\0')
            || label.contains('\0')
        {
            return Err(Error::InvalidInput(
                "database and label must be nonempty without NUL".into(),
            ));
        }
        let invalid = |message| Error::LoadObservation {
            label: label.to_owned(),
            message,
        };
        let transport = |source| Error::Transport {
            label: Some(label.to_owned()),
            source,
        };
        let mut url = self.endpoint.clone();
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .map_err(|_| invalid("invalid FE endpoint"))?
            .pop_if_empty()
            .extend(["api", database, "get_load_state"]);
        url.query_pairs_mut().append_pair("label", label);
        let mut response = self
            .http
            .get(url)
            .basic_auth(&self.username, self.password.as_deref())
            .send()
            .await
            .map_err(transport)?
            .error_for_status()
            .map_err(transport)?;
        if !response.status().is_success() {
            return Err(invalid("observation redirects are not followed"));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if chunk.len() > 65_536 - body.len() {
                return Err(invalid("observation response exceeds the size limit"));
            }
            body.extend_from_slice(&chunk);
        }
        let reply: Reply =
            serde_json::from_slice(&body).map_err(|_| invalid("invalid observation response"))?;
        if reply.code != 0 || reply.data.is_empty() {
            return Err(invalid("observation API did not report a state"));
        }
        let state = match reply.data.as_str() {
            "UNKNOWN" => LabelState::Unknown,
            "PREPARE" => LabelState::Prepare,
            "PRECOMMITTED" => LabelState::Precommitted,
            "COMMITTED" => LabelState::Committed,
            "VISIBLE" => LabelState::Visible,
            "ABORTED" => LabelState::Aborted,
            _ => LabelState::Other(reply.data),
        };
        Ok(LabelObservation {
            database: database.to_owned(),
            label: label.to_owned(),
            state,
        })
    }
}

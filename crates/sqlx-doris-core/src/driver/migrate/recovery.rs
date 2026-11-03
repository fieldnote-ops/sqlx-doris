//! Doris migration recovery operations.
use super::*;

impl DorisConnection {
    pub(super) async fn check_recovery_owners(
        &mut self,
        database: &str,
        owner: &str,
        lock_owner: Option<&str>,
    ) -> Result<(), Error> {
        if self.gate_owner(database).await?.as_deref() != Some(owner) {
            return Err(error(
                "transition owner changed or no such gate exists; inspect lock status",
            ));
        }
        if self.lock_owner(database).await?.as_deref() != lock_owner {
            return Err(error(
                "main lock owner changed; inspect lock status and repeat the maintenance review",
            ));
        }
        Ok(())
    }

    /// Read-only preflight for explicit transition-gate recovery. Returns the
    /// selected database. `lock_owner = None` requires the main lock to be absent.
    /// Both exact owners must match, and all COLUMN/ROLLUP jobs must be terminal.
    /// This cannot prove that an old process or command has stopped.
    pub async fn check_migration_transition_recovery(
        &mut self,
        owner: &str,
        lock_owner: Option<&str>,
    ) -> Result<String, Error> {
        self.ready()?;
        if self.transaction || self.migration_running || self.migration_lock.is_some() {
            return Err(error(
                "transition recovery requires a separate idle maintenance connection",
            ));
        }
        for token in std::iter::once(owner).chain(lock_owner) {
            uuid::Uuid::parse_str(token)
                .map_err(|_| error("recovery requires exact owner UUIDs"))?;
        }
        let database = self.migration_database().await?;
        self.check_recovery_owners(&database, owner, lock_owner)
            .await?;
        for kind in ["COLUMN", "ROLLUP"] {
            let rows = self
                .fetch_all(trusted(format!(
                    "SHOW ALTER TABLE {kind} FROM {}",
                    quote(&database)?
                )))
                .await?;
            for row in rows {
                let state: &str = row.try_get("State")?;
                if !matches!(state, "FINISHED" | "CANCELLED") {
                    return Err(error(format!(
                        "transition recovery blocked by {kind} job on {} in state {state}; reconcile SHOW ALTER jobs first",
                        row.try_get::<&str, _>("TableName")?
                    )));
                }
            }
        }
        // Job inspection can take time; reject changes observed during it.
        self.check_recovery_owners(&database, owner, lock_owner)
            .await?;
        Ok(database)
    }

    /// Remove only a reconciled transition gate during externally serialized
    /// maintenance. Stop ALL migration, unlock and recovery actors, ensure no
    /// old submitted commands can still commit, and allow only one administrator.
    /// The owner UUID is encoded in an exclusive-range partition name. DROP
    /// PARTITION atomically resolves that name, so a successor partition cannot
    /// be removed by a delayed cleanup. Maintenance is still required for old
    /// business SQL; metadata fencing does not stop an active runner's queries.
    ///
    /// Requires the exact transition owner and expected main owner (`None` means
    /// absent), rejects nonterminal schema jobs, and preserves the main lock and
    /// migration records. Never retries DROP. Once removal starts, the connection
    /// is retired on success, error or cancellation. After an uncertain result,
    /// inspect lock status using a new connection before taking another action.
    pub async fn recover_migration_transition_in_maintenance(
        &mut self,
        owner: &str,
        lock_owner: Option<&str>,
    ) -> Result<(), Error> {
        self.recovery_drop_phase = MigrationRecoveryPhase::NotSubmitted;
        let database = self
            .check_migration_transition_recovery(owner, lock_owner)
            .await
            .map_err(|e| error(format!("transition recovery stopped before DROP; no DROP sent: {e}; inspect lock status before retrying")))?;
        let table = format!("{}.{}", quote(&database)?, quote(GATE_TABLE)?);
        let removal = MaintenanceRemoval(self);
        removal
            .0
            .check_recovery_owners(&database, owner, lock_owner)
            .await
            .map_err(|e| error(format!("transition recovery stopped before DROP; no DROP sent: {e}; use lock status on a new connection")))?;
        let sql = owner_drop(&table, owner)?;
        removal.0.recovery_drop_phase = MigrationRecoveryPhase::Submitted;
        removal
            .0
            .execute(sql)
            .await
            .map_err(|e| error(format!("transition DROP outcome is uncertain: {e}; use lock status on a new connection; do not replay migrations")))?;
        removal.0.recovery_drop_phase = MigrationRecoveryPhase::Acknowledged;
        let verification = async {
            if removal.0.gate_owner(&database).await?.is_some()
                || removal.0.lock_owner(&database).await?.as_deref() != lock_owner
            {
                return Err(error("lock state changed during maintenance"));
            }
            Ok(())
        }
        .await;
        if verification.is_ok() {
            removal.0.recovery_drop_phase = MigrationRecoveryPhase::Verified;
        }
        verification.map_err(|e: Error| error(format!("transition DROP acknowledged; state verification failed: {e}; stop all actors and use lock status on a new connection; do not replay migrations")))
    }

    /// Recover a durable lock only after its runner has stopped and submitted
    /// DDL jobs have been reconciled. The exact current owner token is required.
    /// Ownership checks and deletion serialize with normal unlock and recovery.
    pub async fn recover_migration_lock(&mut self, owner: &str) -> Result<(), Error> {
        self.recovery_drop_phase = MigrationRecoveryPhase::NotSubmitted;
        uuid::Uuid::parse_str(owner).map_err(|_| error("recovery requires an exact owner UUID"))?;
        if self.transaction || self.migration_running {
            return Err(error(
                "cannot recover a lock during a transaction or migration",
            ));
        }
        let mut gate = self.transition().await?;
        let result = async {
            if gate.conn.lock_owner(&gate.database).await?.as_deref() != Some(owner) {
                return Err(error("lock owner changed or no such lock exists"));
            }
            let table = format!("{}.{}", quote(&gate.database)?, quote(LOCK_TABLE)?);
            gate.remove_main(&table, owner).await?;
            if gate
                .conn
                .migration_lock
                .as_ref()
                .is_some_and(|l| l.token == owner)
            {
                gate.conn.migration_lock = None;
            }
            Ok(())
        }
        .await;
        gate.finish(result).await?;
        self.recovery_drop_phase = MigrationRecoveryPhase::Verified;
        Ok(())
    }

    /// Observe the latest main-lock release or explicit recovery DROP stage.
    /// Read immediately after recovery; later normal release can update it.
    /// NotSubmitted does not attest that coordination metadata was unchanged.
    pub fn migration_recovery_phase(&self) -> MigrationRecoveryPhase {
        self.recovery_drop_phase
    }
}

//! Short-lived, per-user visibility into an in-flight PlaybackInfo probe.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct StartupProbeTracker(Arc<Mutex<HashMap<(Uuid, Uuid), ProbeSnapshot>>>);

#[derive(Clone, Debug)]
pub struct ProbeSnapshot {
    pub request_id: Uuid,
    pub source_id: Option<Uuid>,
    pub candidate_index: usize,
    pub candidate_count: usize,
    pub probe_budget_seconds: u64,
    pub started_at: Instant,
    pub source_started_at: Instant,
}

pub struct ProbeGuard {
    tracker: StartupProbeTracker,
    key: (Uuid, Uuid),
    request_id: Uuid,
}

impl StartupProbeTracker {
    pub fn begin(&self, user_id: Uuid, item_id: Uuid) -> ProbeGuard {
        let now = Instant::now();
        let request_id = Uuid::new_v4();
        let key = (user_id, item_id);
        if let Ok(mut probes) = self
            .0
            .lock()
        {
            probes.insert(
                key,
                ProbeSnapshot {
                    request_id,
                    source_id: None,
                    candidate_index: 0,
                    candidate_count: 0,
                    probe_budget_seconds: 0,
                    started_at: now,
                    source_started_at: now,
                },
            );
        }
        ProbeGuard {
            tracker: self.clone(),
            key,
            request_id,
        }
    }

    pub fn snapshot(&self, user_id: Uuid, item_id: Uuid) -> Option<ProbeSnapshot> {
        self.0
            .lock()
            .ok()?
            .get(&(user_id, item_id))
            .filter(|probe| {
                probe
                    .started_at
                    .elapsed()
                    <= Duration::from_secs(10 * 60)
            })
            .cloned()
    }
}

impl ProbeGuard {
    pub fn update(
        &self,
        source_id: Uuid,
        index: usize,
        count: usize,
        budget_seconds: u64,
    ) {
        if let Ok(mut probes) = self
            .tracker
            .0
            .lock()
        {
            if let Some(probe) = probes
                .get_mut(&self.key)
                .filter(|probe| probe.request_id == self.request_id)
            {
                probe.source_id = Some(source_id);
                probe.candidate_index = index;
                probe.candidate_count = count;
                probe.probe_budget_seconds = budget_seconds;
                probe.source_started_at = Instant::now();
            }
        }
    }
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if let Ok(mut probes) = self
            .tracker
            .0
            .lock()
        {
            if probes
                .get(&self.key)
                .is_some_and(|probe| probe.request_id == self.request_id)
            {
                probes.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_is_scoped_to_user_and_latest_request() {
        let tracker = StartupProbeTracker::default();
        let user = Uuid::new_v4();
        let other = Uuid::new_v4();
        let item = Uuid::new_v4();
        let first = tracker.begin(user, item);
        first.update(Uuid::new_v4(), 1, 3, 60);
        assert!(
            tracker
                .snapshot(other, item)
                .is_none()
        );
        let second = tracker.begin(user, item);
        drop(first);
        assert_eq!(
            tracker
                .snapshot(user, item)
                .unwrap()
                .candidate_index,
            0
        );
        second.update(Uuid::new_v4(), 2, 3, 60);
        assert_eq!(
            tracker
                .snapshot(user, item)
                .unwrap()
                .candidate_index,
            2
        );
        drop(second);
        assert!(
            tracker
                .snapshot(user, item)
                .is_none()
        );
    }
}

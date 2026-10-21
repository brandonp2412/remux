use anyhow::Result;
use async_trait::async_trait;
use remux_sdks::remux::MediaSegments;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

use super::{
    AddonCapabilities, AddonKind, AddonMetadata, AddonPreset, AddonPresetRegistration,
    MediaKind, ResourceType, SegmentAddon,
};
use crate::{AppContext, db};

pub struct ProbePreset;

impl AddonPreset for ProbePreset {
    fn id(&self) -> &'static str {
        "probe"
    }

    fn metadata(&self) -> AddonMetadata {
        AddonMetadata {
            id: "probe".to_string(),
            display_name: "Probe Segments".to_string(),
            description:
                "Extracts segment markers (intro, outro, chapters) from the media file's probe data. Includes probe data from remuxdb if enabled."
                    .to_string(),
            icon: None,
            supported_resources: vec![AddonMetadata::simple_resource(
                ResourceType::Segment,
            )],
            supported_types: vec![MediaKind::Movie, MediaKind::Episode],
            supported_resources_user: vec![],
            supported_types_user: vec![],
            options: vec![],
        }
    }

    fn from_cfg(
        &self,
        _addon_id: Uuid,
        _cfg: &serde_json::Value,
        _config: &crate::Config,
    ) -> Result<AddonCapabilities> {
        let addon = Arc::new(ProbeAddon {});
        Ok(AddonCapabilities {
            kind: Some(addon.clone()),
            segment: Some(addon),
            ..Default::default()
        })
    }
}

inventory::submit! {
    AddonPresetRegistration(|| Box::new(ProbePreset))
}

const TICKS_PER_SECOND: i64 = 10_000_000;
const MAX_PROBED_INTRO_END_TICKS: i64 = 8 * 60 * TICKS_PER_SECOND;

fn discard_implausible_cached_intro(
    segments: &mut MediaSegments,
    runtime_secs: Option<i64>,
) {
    let Some(intro) = segments
        .intro
        .as_ref()
    else {
        return;
    };
    let runtime_ticks =
        runtime_secs.and_then(|seconds| seconds.checked_mul(TICKS_PER_SECOND));
    let invalid = intro.start_ticks < 0
        || intro.end_ticks <= intro.start_ticks
        || intro.end_ticks > MAX_PROBED_INTRO_END_TICKS
        || runtime_ticks.is_some_and(|runtime| intro.end_ticks > runtime / 3);
    if invalid {
        warn!(
            start_ticks = intro.start_ticks,
            end_ticks = intro.end_ticks,
            ?runtime_ticks,
            "discarding implausible cached probe intro segment"
        );
        segments.intro = None;
    }
}

pub struct ProbeAddon {}

#[async_trait]
impl AddonKind for ProbeAddon {
    fn id(&self) -> &'static str {
        "probe"
    }
}

#[async_trait]
impl SegmentAddon for ProbeAddon {
    fn supports(&self, media: &db::Media) -> bool {
        matches!(media.kind, db::MediaKind::Episode | db::MediaKind::Movie)
    }

    async fn segment_fetch(
        &self,
        media: &db::Media,
        ctx: &AppContext,
    ) -> Result<MediaSegments> {
        let sources = db::Media::get_by_filter(
            &ctx.db,
            &db::MediaFilter {
                parent_id: Some(media.id),
                kind: Some(vec![db::MediaKind::Stream]),
                ..Default::default()
            },
        )
        .await
        .map(|r| r.records)
        .unwrap_or_default();

        let mut merged = MediaSegments::default();
        for source in sources {
            if let Some(probe) = source.probe_data {
                if let Some(mut segs) = probe.segments {
                    discard_implausible_cached_intro(&mut segs, media.runtime);
                    merged.merge_from(segs);
                }
            }
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remux_sdks::remux::Segment;

    #[test]
    fn cached_probe_rejects_tatami_intro_that_spans_the_episode() {
        let mut segments = MediaSegments {
            intro: Some(Segment {
                start_ticks: 10_010_000,
                end_ticks: 9_969_550_000,
            }),
            outro: Some(Segment {
                start_ticks: 9_969_550_000,
                end_ticks: 10_879_680_000,
            }),
            ..Default::default()
        };

        discard_implausible_cached_intro(&mut segments, Some(1080));

        assert_eq!(segments.intro, None);
        assert!(
            segments
                .outro
                .is_some()
        );
    }

    #[test]
    fn cached_probe_keeps_normal_intro() {
        let expected = Segment {
            start_ticks: 10_000_000,
            end_ticks: 900_000_000,
        };
        let mut segments = MediaSegments {
            intro: Some(expected.clone()),
            ..Default::default()
        };

        discard_implausible_cached_intro(&mut segments, Some(1080));

        assert_eq!(segments.intro, Some(expected));
    }
}

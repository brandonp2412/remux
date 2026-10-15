use crate::{AppState, db::auth};
use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use axum_anyhow::ApiResult as Result;
use remux_macros::{delete, get, post};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct DownloadRequest {
    magnet: String,
    advertised_height: u32,
}

#[get("/remux/downloads")]
pub async fn download_list(
    State(state): State<AppState>,
    session: auth::AuthSession,
) -> Result<impl IntoResponse> {
    let manager = state
        .ctx
        .torrent
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))?;
    Ok(Json(
        manager
            .list_background_downloads(
                &session
                    .user
                    .id
                    .to_string(),
            )
            .await,
    ))
}

#[post("/remux/downloads")]
pub async fn download_create(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Json(request): Json<DownloadRequest>,
) -> Result<impl IntoResponse> {
    let manager = state
        .ctx
        .torrent
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))?;
    Ok(Json(
        manager
            .create_background_download(
                &session
                    .user
                    .id
                    .to_string(),
                &request.magnet,
                request.advertised_height,
            )
            .await?,
    ))
}

#[post("/remux/downloads/{hash}/pause")]
pub async fn download_pause(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse> {
    let manager = state
        .ctx
        .torrent
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))?;
    Ok(Json(
        manager
            .set_background_download_paused(
                &session
                    .user
                    .id
                    .to_string(),
                &hash,
                true,
            )
            .await?,
    ))
}

#[post("/remux/downloads/{hash}/resume")]
pub async fn download_resume(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse> {
    let manager = state
        .ctx
        .torrent
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))?;
    Ok(Json(
        manager
            .set_background_download_paused(
                &session
                    .user
                    .id
                    .to_string(),
                &hash,
                false,
            )
            .await?,
    ))
}

#[delete("/remux/downloads/{hash}")]
pub async fn download_delete(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(hash): Path<String>,
) -> Result<impl IntoResponse> {
    let manager = state
        .ctx
        .torrent
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow::anyhow!("P2P is disabled"))?;
    manager
        .delete_background_download(
            &session
                .user
                .id
                .to_string(),
            &hash,
        )
        .await?;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

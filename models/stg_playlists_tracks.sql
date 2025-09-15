-- Staging: explode playlists.tracks
{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw', 'playlists') }}
)
select
  playlist_id,
  owner_id,
  title as playlist_title,
  track.track_id,
  track.title as track_title,
  track.duration_sec
from src, unnest(tracks) as track

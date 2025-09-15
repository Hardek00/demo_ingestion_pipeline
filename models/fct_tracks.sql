-- Fact: one row per track on a playlist
{{ config(materialized='table') }}

select * from {{ ref('stg_playlists_tracks') }}

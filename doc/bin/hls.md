---
title: HLS
description: Serve a broadcast as HLS or DASH, or import an HLS playlist
---

# HLS

`moq export hls` serves a MoQ broadcast as HLS and DASH over HTTP for players
that can't speak MoQ. `moq import hls` pulls a remote HLS master or media
playlist into a broadcast.

```bash
# Publish with HLS records, then serve. Players open
# http://localhost:8089/my-stream.hang/master.m3u8 (or manifest.mpd for DASH)
ffmpeg -re -i input.mp4 -c copy -f mp4 -movflags cmaf+separate_moof+delay_moov+skip_trailer -frag_duration 1000 - | moq --connect https://relay.example.com/anon --broadcast my-stream.hang import --hls fmp4
moq --connect https://relay.example.com/anon --broadcast my-stream.hang export hls --listen '[::]:8089'

# Import
moq --connect https://relay.example.com/anon --broadcast my-stream.hang import hls https://example.com/live/master.m3u8
```

The CLI serves the one `--broadcast` it was given, at
`/{broadcast}/master.m3u8` and `/{broadcast}/manifest.mpd`.

Import follows a live playlist, joining a few segments behind its live edge,
until every rendition's playlist ends with `EXT-X-ENDLIST`. It then keeps the
finished broadcast up for `--linger`
(see [retention](/bin/cli#retention-and-latency)). A playlist that has already
ended, such as a VOD recording, is refused with an error rather than imported.
An `EXT-X-DISCONTINUITY` keeps the source's timestamps. Timestamp resets are not
supported: if a rendition's timestamps go backwards under the same `EXT-X-MAP`,
such as an encoder restart, the import fails with an error. Import the restarted
source as a new broadcast. Import fetches only from the playlist's origin
(scheme, host, and port): a reference or redirect to another origin, such as a
separate CDN host, fails the import.

## How export works

Export never subscribes to media. It reads each rendition's
[HLS records](/concept/hang#catalog), a small track listing where each of
that rendition's segments starts, to build playlists. A segment request fetches
exactly the groups it covers from the relay's cache and transmuxes them to CMAF.
Media is never transcoded, so the player must support the publisher's codecs.

What that means for you:

- **Opt in at the publisher.** Only a rendition whose catalog entry carries `hls` gets a playlist; nothing publishes it by default. Pass `--hls` to `moq import` (the stdin formats, `hls`, `rtsp`, and `capture`), or enable it with `moq_mux::catalog::Config::with_hls` in Rust. Without a duration hint, a rendition declares its records after its first segment.
- **Align keyframes across renditions.** Segment `n` of every rendition comes from that rendition's record `n`, so a publisher that opts in must start its renditions' segments at the same timestamps, and start them together. A rendition's segment that does not line up with the lead rendition's (the first enabled video rendition, or audio when there is none) is listed as `EXT-X-GAP`, left out of the DASH manifest, and never served.
- **Idle renditions cost nothing**, and a segment is servable for as long as the relay's [cache](/bin/relay/config#cache) retains it. Keep `--window` (the playlist length, default 16 s) within that retention.
- **Live joins cost the window.** The exporter reads the record checkpoint and lists at most 256 segments.
- **Segments are timed by their records.** A segment is listed once the record after it arrives, and its duration, its DASH `t` and `d`, and its last sample's duration all come from its own record and the next, so a segment's bytes never change with a catalog estimate. A stop record (a break or the end of the content) is listed as a zero-length gap, and what follows it starts a discontinuity.
- **Disabled renditions are hidden.** A rendition the catalog marks `enabled: false` leaves the multivariant playlist and DASH manifest until it re-enables. Its media playlist keeps serving, so a player already on it is not ended with `EXT-X-ENDLIST`.
- **URLs name the publisher instance.** Every URL the multivariant playlist and DASH manifest list sits under `@<instance>`, the publisher's epoch, so every edge and every reload derive the same URLs and a CDN in front of export caches them. A restarted publisher gets URLs of its own, whose numbering starts over. The multivariant playlist and manifest switch to it at once, while a player already on the old instance keeps playing until that publisher leaves; the player then has to reload the multivariant playlist. An instance without an epoch (one that reached export over moq-lite-06 or older) gets an id each export mints, so only that export serves its URLs.
- **Only a clean finish ends a playlist.** `EXT-X-ENDLIST` and a `static` DASH manifest mean the records finished. A rendition whose records fail instead, such as a publisher dropping its session, answers HTTP 500 with `Cache-Control: no-store`, so a truncated recording never looks complete, and leaves the multivariant playlist and DASH manifest so players switch to a sibling. With none left, the DASH manifest answers the error too.

A [`moq-archive`](https://docs.rs/moq-archive) recording is served the same
way, with no second stored copy: record a broadcast, republish it, and export
the republished broadcast.

```bash
moq --connect https://relay.example.com/anon --broadcast event export archive s3://recordings
moq --connect https://relay.example.com/anon --broadcast event-replay.hang import archive s3://recordings/event/@<epoch>
moq --connect https://relay.example.com/anon --broadcast event-replay.hang export hls --listen '[::]:8089'
```

The archived catalog and HLS records replay as recorded, so the export lists
the same window as a live broadcast.

`--listen-tls-cert`/`--listen-tls-key` or `--listen-tls-generate` serve HTTPS,
and `--cors-origin` opens it to browsers. HLS TLS reads `MOQ_HLS_TLS_*`;
`MOQ_LISTEN_TLS_*` configures only the MoQ listener. See `moq export hls --help`.

## Import

Import handles classic HLS; LL-HLS parts are not implemented. It publishes the
playlist's own media times, and anchors the catalog clock on the first imported
segment. Each fMP4 segment is split into one frame per sample; `--cmaf`
publishes each fragment whole as CMAF instead, keeping its exact sample timing.

The library, including the recorder cursors for mirroring a broadcast to
storage, is [`moq-hls`](https://docs.rs/moq-hls).

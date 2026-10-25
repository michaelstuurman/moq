# [M] HLS records number late renditions from the pacer

## Goal

A rendition enrolled for HLS records after its siblings, or re-enrolled after
a reconfigure, numbers its records so that record `n` starts at the same pts as
segment `n` of the renditions already running. Today `moq-mux`'s `hls::Tracks`
gives a new follower sequence 0 (a re-enrolled name continues its own count),
while an edge maps record `n` to segment `n` and needs the window index to
equal the sequence, so every segment of a late rendition is listed as an
`EXT-X-GAP` until the counts happen to meet.

## Plan

- Start a follower's window at the pacer's next sequence (a window offset),
  so its first record carries the pacer's number for that boundary.
- Keep the record track's window index equal to `sequence`, which the
  consumers refuse otherwise.
- Test a rendition added mid-broadcast and one rebuilt by a reconfigure: both
  list aligned segments, without gaps, from their first record.

Public API: none expected. Wire: none.

## Related

- [HLS and DASH segment conformance](/quest/m1/hls-conformance.md) - checks the manifests this numbering feeds

# [M] A cancelled moq-transport request stops writing

## Goal

Cancelling a moq-transport SUBSCRIBE, FETCH, TRACK_STATUS, or namespace
request releases its state at once, even when the peer withholds send
credit.

## Plan

Every request write in `rs/moq-net/src/ietf/subscriber.rs` awaits its first
message without watching for cancellation, so a peer that withholds credit
pins a cancelled request.
Lazy discovery ([moq-dev/moq#6530](https://github.com/moq-dev/moq/pull/6530))
fixed only the stream-open race. Every remaining draft gives each request its
own stream, so reset the partially written request stream.

Test with a peer that never grants credit, on drafts 18 and 22.

Public API: none. Wire: none.

# stream_multiplexer

[![Build Status][actions_badge]][actions]
[![Latest Version][crates_badge]][crates]
[![Rust Documentation][docs_badge]][docs]

This crate provides natural backpressure to classes of streams

Streams are gathered into 'channels' that can be polled via `recv()`. Channels are indpendent
of each other and have their own backpressure.

[docs_badge]: https://docs.rs/stream_multiplexer/badge.svg
[docs]: https://docs.rs/stream_multiplexer
[crates_badge]: https://img.shields.io/crates/v/stream_multiplexer.svg
[crates]: https://crates.io/crates/stream_multiplexer
[actions_badge]: https://github.com/halzy/stream_multiplexer/workflows/Rust/badge.svg
[actions]: https://github.com/halzy/stream_multiplexer/actions


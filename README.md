# stream_multiplexer

[![Build Status][actions_badge]][actions]
[![Latest Version][crates_badge]][crates]
[![Rust Documentation][docs_badge]][docs]

Highly unstable API!

This library multiplexes many streams into fewer streams.
New streams are assigned an identifier. Data from those streams are wrapped in a data structure that contains the Id and Bytes, and then funneled into another stream. 

[docs_badge]: https://docs.rs/stream_multiplexer/badge.svg
[docs]: https://docs.rs/stream_multiplexer
[crates_badge]: https://img.shields.io/crates/v/stream_multiplexer.svg
[crates]: https://crates.io/crates/stream_multiplexer
[actions_badge]: https://github.com/halzy/stream_multiplexer/workflows/Rust/badge.svg
[actions]: https://github.com/halzy/stream_multiplexer/actions


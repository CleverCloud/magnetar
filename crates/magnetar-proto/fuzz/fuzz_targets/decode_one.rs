// SPDX-License-Identifier: Apache-2.0
//
// Fuzz target: feed arbitrary bytes into `magnetar_proto::frame::decode_one`
// and assert it does not panic. Any panic is a bug.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut src = Bytes::copy_from_slice(data);
    // Loop-decode in case the input contains multiple framed messages.
    // Stop as soon as the decoder reports incomplete or any error — we only
    // assert that *panics* are absent.
    while !src.is_empty() {
        match magnetar_proto::frame::decode_one(&mut src) {
            Ok(_frame) => continue,
            Err(_err) => break,
        }
    }
});

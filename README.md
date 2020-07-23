# async-executor

[![Build](https://github.com/stjepang/async-executor/workflows/Build%20and%20test/badge.svg)](
https://github.com/stjepang/async-executor/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](
https://github.com/stjepang/async-executor)
[![Cargo](https://img.shields.io/crates/v/async-executor.svg)](
https://crates.io/crates/async-executor)
[![Documentation](https://docs.rs/async-executor/badge.svg)](
https://docs.rs/async-executor)

Async executor.

This crate offers two kinds of executors: single-threaded and multi-threaded.

## Examples

Run a single-threaded and a multi-threaded executor at the same time:

```rust
use async_channel::unbounded;
use async_executor::{Executor, LocalExecutor};
use easy_parallel::Parallel;

let ex = Executor::new();
let local_ex = LocalExecutor::new();
let (trigger, shutdown) = unbounded::<()>();

Parallel::new()
    // Run four executor threads.
    .each(0..4, |_| ex.run(shutdown.recv()))
    // Run local executor on the current thread.
    .finish(|| local_ex.run(async {
        println!("Hello world!");
        drop(trigger);
    }));
```

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

#### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

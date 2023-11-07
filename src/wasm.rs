//! WebAssembly implementation of the executor.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_task::{Runnable, Task};
use js_sys::Promise;
use wasm_bindgen::prelude::*;

/// The state of the executor.
pub(super) struct State {

}

impl State {
    /// Create a new, empty executor state.
    #[inline]
    pub(crate) fn new() -> Self {

    }
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen]
    fn queueMicrotask(closure: &Closure<dyn FnMut(JsValue)>);

    type Global;

    #[wasm_bindgen(method, getter, js_name = queueMicrotask)]
    fn hasQueueMicrotask(this: &Global) -> JsValue;
}


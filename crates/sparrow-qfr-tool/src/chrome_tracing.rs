//! Structs representing chrome traces.
//!
//! The serde-json format produces Chrome tracing JSON, as described in docs below.
//!
//! <https://aras-p.info/blog/2017/01/23/Chrome-Tracing-as-Profiler-Frontend/>
//!
//! <https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU>

mod args;
mod categories;
mod stack_frame;
mod trace;
mod trace_event;

pub(crate) use args::*;
pub(crate) use stack_frame::*;
pub(crate) use trace::*;
pub(crate) use trace_event::*;

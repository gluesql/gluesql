#[cfg(not(feature = "send"))]
pub use std::rc::Rc;
#[cfg(feature = "send")]
pub use std::sync::Arc as Rc;

#[cfg(feature = "send")]
pub use im::{HashMap, HashSet};
#[cfg(not(feature = "send"))]
pub use im_rc::{HashMap, HashSet};

pub trait SendSync {}
#[cfg(feature = "send")]
impl<T: Send + Sync> SendSync for T {}
#[cfg(not(feature = "send"))]
impl<T> SendSync for T {}

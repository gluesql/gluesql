#[cfg(not(feature = "send"))]
pub use std::rc::Rc;
#[cfg(feature = "send")]
pub use std::sync::Arc as Rc;

#[cfg(feature = "send")]
pub use im::{HashMap, HashSet};
#[cfg(not(feature = "send"))]
pub use im_rc::{HashMap, HashSet};

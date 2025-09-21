// Console debug function that outputs to Node.js console
pub fn console_debug(msg: &str) {
  #[cfg(debug_assertions)]
  {
    // In Node.js environment, eprintln! goes to stderr which appears in console
    // This mimics JavaScript's console.debug behavior
    eprintln!("{}", msg);
  }
}

// Set panic hook for better error reporting
#[allow(dead_code)]
pub fn set_panic_hook() {
  #[cfg(feature = "console_error_panic_hook")]
  console_error_panic_hook::set_once();
}

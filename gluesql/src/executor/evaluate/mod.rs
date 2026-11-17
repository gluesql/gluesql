// ...existing code...

// Replace the old closure definitions for eval_to_str, eval_to_float, eval_to_integer:
let eval_to_str = |e: Evaluated| -> Result<String> { e.try_into() };
let eval_to_float = |e: Evaluated| -> Result<f64> { e.try_into() };
let eval_to_integer = |e: Evaluated| -> Result<i64> { e.try_into() };

// ...existing code...


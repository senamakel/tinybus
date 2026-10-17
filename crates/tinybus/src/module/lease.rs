//! A test-only landing point for the acknowledged module runtime lease.
//!
//! The public API and implementation are intentionally withheld until the
//! prerequisite host-object ownership API is merged and published.

#[cfg(test)]
#[path = "lease_tests.rs"]
mod tests;

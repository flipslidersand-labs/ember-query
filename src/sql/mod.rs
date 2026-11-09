pub mod ast;
pub mod parser;

pub use parser::parse;

/// Normalize an identifier to NFC so that CSV headers and SQL identifiers match
/// regardless of the composition form (e.g. NFD headers from macOS). Table names
/// used for file lookup are NOT normalized; only column names, aliases and tags.
pub fn nfc(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    s.nfc().collect()
}

//! These integer result codes match sqlite's definitions in
//! <https://www.sqlite.org/rescode.html> and
//! <https://www.sqlite.org/c3ref/c_abort.html>.

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum SqliteCode {
    Ok = 0,
    Error = 1,
    Internal = 2,
    CantOpen = 14,
}

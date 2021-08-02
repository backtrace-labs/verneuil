//! These integer lock levels are checked in `c/vfs.c`

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum LockLevel {
    None,
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

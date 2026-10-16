//! C ABI for lsm-engine. See `include/lsm.h` for the contract.
//!
//! Safety rules applied to every exported function:
//! * Every pointer argument is checked for NULL before use; a NULL pointer
//!   with a non-zero length is `LSM_ERR_INVALID_ARGUMENT`.
//! * The body runs inside `catch_unwind`, so a Rust panic never unwinds
//!   across the FFI boundary; it becomes `LSM_ERR_PANIC`.
//! * Buffers handed to C are `Box<[u8]>` turned into (ptr, len) and are
//!   only freed through `lsm_free_buf` with the same length.

#![allow(clippy::missing_safety_doc, non_camel_case_types)]

use lsm_engine::fs::SimFs;
use lsm_engine::{Db, DbIterator, Error, Options, ReadOptions, Snapshot, WriteBatch};
use std::ffi::{CStr, CString, c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;

pub const LSM_OK: c_int = 0;
pub const LSM_NOT_FOUND: c_int = 1;
pub const LSM_ERR_INVALID_ARGUMENT: c_int = -1;
pub const LSM_ERR_IO: c_int = -2;
pub const LSM_ERR_CORRUPTION: c_int = -3;
pub const LSM_ERR_PANIC: c_int = -4;

pub struct lsm_db {
    db: Arc<Db>,
}

pub struct lsm_snapshot {
    snap: Snapshot,
}

pub struct lsm_batch {
    batch: WriteBatch,
}

pub struct lsm_iter {
    it: DbIterator,
    key: Vec<u8>,
    val: Vec<u8>,
}

struct Failure(c_int, String);

impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        let code = match &e {
            Error::Io(_) => LSM_ERR_IO,
            Error::Corruption(_) => LSM_ERR_CORRUPTION,
            Error::InvalidArgument(_) => LSM_ERR_INVALID_ARGUMENT,
        };
        Failure(code, e.to_string())
    }
}

fn invalid(msg: &str) -> Failure {
    Failure(LSM_ERR_INVALID_ARGUMENT, msg.to_string())
}

unsafe fn set_err(errmsg: *mut *mut c_char, msg: &str) {
    if errmsg.is_null() {
        return;
    }
    let c = CString::new(msg.replace('\0', " ")).unwrap_or_default();
    unsafe { *errmsg = c.into_raw() };
}

/// Runs `f`, converting errors and panics into a status code.
unsafe fn guard(errmsg: *mut *mut c_char, f: impl FnOnce() -> Result<c_int, Failure>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(code)) => code,
        Ok(Err(Failure(code, msg))) => {
            unsafe { set_err(errmsg, &msg) };
            code
        }
        Err(p) => {
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".into());
            unsafe { set_err(errmsg, &format!("panic: {msg}")) };
            LSM_ERR_PANIC
        }
    }
}

/// Borrows (ptr, len) as a slice. NULL is allowed only for len 0.
unsafe fn bytes<'a>(p: *const u8, len: usize, what: &str) -> Result<&'a [u8], Failure> {
    if p.is_null() {
        if len == 0 {
            return Ok(&[]);
        }
        return Err(invalid(&format!("{what} is NULL but length is {len}")));
    }
    if len > isize::MAX as usize {
        return Err(invalid(&format!("{what} length too large")));
    }
    Ok(unsafe { std::slice::from_raw_parts(p, len) })
}

unsafe fn db_ref<'a>(db: *mut lsm_db) -> Result<&'a lsm_db, Failure> {
    if db.is_null() {
        return Err(invalid("db is NULL"));
    }
    Ok(unsafe { &*db })
}

fn open_with(opts: Options, path: &str) -> Result<*mut lsm_db, Failure> {
    let db = Db::open(path, opts)?;
    Ok(Box::into_raw(Box::new(lsm_db { db: Arc::new(db) })))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_open(
    path: *const c_char,
    sync: c_int,
    out: *mut *mut lsm_db,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            if path.is_null() || out.is_null() {
                return Err(invalid("path and out must not be NULL"));
            }
            let path = CStr::from_ptr(path)
                .to_str()
                .map_err(|_| invalid("path is not valid UTF-8"))?;
            let opts = Options {
                sync: sync != 0,
                ..Default::default()
            };
            *out = open_with(opts, path)?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_open_in_memory(
    sync: c_int,
    out: *mut *mut lsm_db,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            if out.is_null() {
                return Err(invalid("out must not be NULL"));
            }
            let opts = Options {
                sync: sync != 0,
                fs: Arc::new(SimFs::new()),
                ..Default::default()
            };
            *out = open_with(opts, "/mem")?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_close(db: *mut lsm_db) {
    if db.is_null() {
        return;
    }
    // Dropping joins the background thread; never let a panic escape.
    let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(db) })));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_put(
    db: *mut lsm_db,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            let db = db_ref(db)?;
            let k = bytes(key, key_len, "key")?;
            let v = bytes(val, val_len, "value")?;
            db.db.put(k, v)?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_delete(
    db: *mut lsm_db,
    key: *const u8,
    key_len: usize,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            let db = db_ref(db)?;
            db.db.delete(bytes(key, key_len, "key")?)?;
            Ok(LSM_OK)
        })
    }
}

fn read_opts(snap: *const lsm_snapshot) -> ReadOptions<'static> {
    if snap.is_null() {
        ReadOptions::default()
    } else {
        // The caller guarantees the snapshot outlives this call.
        ReadOptions {
            snapshot: Some(unsafe { &(*snap).snap }),
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_get(
    db: *mut lsm_db,
    snap: *const lsm_snapshot,
    key: *const u8,
    key_len: usize,
    val: *mut *mut u8,
    val_len: *mut usize,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            let db = db_ref(db)?;
            if val.is_null() || val_len.is_null() {
                return Err(invalid("val and val_len must not be NULL"));
            }
            *val = ptr::null_mut();
            *val_len = 0;
            let k = bytes(key, key_len, "key")?;
            match db.db.get_opt(k, read_opts(snap))? {
                Some(v) => {
                    let b = v.into_boxed_slice();
                    *val_len = b.len();
                    *val = Box::into_raw(b) as *mut u8;
                    Ok(LSM_OK)
                }
                None => Ok(LSM_NOT_FOUND),
            }
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_flush(db: *mut lsm_db, errmsg: *mut *mut c_char) -> c_int {
    unsafe {
        guard(errmsg, || {
            db_ref(db)?.db.flush()?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_compact(db: *mut lsm_db, errmsg: *mut *mut c_char) -> c_int {
    unsafe {
        guard(errmsg, || {
            db_ref(db)?.db.compact_all()?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lsm_batch_new() -> *mut lsm_batch {
    Box::into_raw(Box::new(lsm_batch {
        batch: WriteBatch::new(),
    }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_batch_put(
    b: *mut lsm_batch,
    key: *const u8,
    key_len: usize,
    val: *const u8,
    val_len: usize,
) -> c_int {
    unsafe {
        guard(ptr::null_mut(), || {
            if b.is_null() {
                return Err(invalid("batch is NULL"));
            }
            let (k, v) = (bytes(key, key_len, "key")?, bytes(val, val_len, "value")?);
            (*b).batch.put(k, v);
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_batch_delete(
    b: *mut lsm_batch,
    key: *const u8,
    key_len: usize,
) -> c_int {
    unsafe {
        guard(ptr::null_mut(), || {
            if b.is_null() {
                return Err(invalid("batch is NULL"));
            }
            (*b).batch.delete(bytes(key, key_len, "key")?);
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_write(
    db: *mut lsm_db,
    b: *const lsm_batch,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            let db = db_ref(db)?;
            if b.is_null() {
                return Err(invalid("batch is NULL"));
            }
            db.db.write((*b).batch.clone())?;
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_batch_free(b: *mut lsm_batch) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_snapshot_new(db: *mut lsm_db) -> *mut lsm_snapshot {
    let r = catch_unwind(AssertUnwindSafe(|| {
        if db.is_null() {
            return ptr::null_mut();
        }
        let snap = unsafe { &*db }.db.snapshot();
        Box::into_raw(Box::new(lsm_snapshot { snap }))
    }));
    r.unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_snapshot_free(s: *mut lsm_snapshot) {
    if !s.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(s) })));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_iter_new(
    db: *mut lsm_db,
    snap: *const lsm_snapshot,
    start: *const u8,
    start_len: usize,
    end: *const u8,
    end_len: usize,
    out: *mut *mut lsm_iter,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            let db = db_ref(db)?;
            if out.is_null() {
                return Err(invalid("out must not be NULL"));
            }
            let start = if start.is_null() {
                if start_len != 0 {
                    return Err(invalid("start is NULL but length is not 0"));
                }
                None
            } else {
                Some(bytes(start, start_len, "start")?)
            };
            let end = if end.is_null() {
                if end_len != 0 {
                    return Err(invalid("end is NULL but length is not 0"));
                }
                None
            } else {
                Some(bytes(end, end_len, "end")?)
            };
            let it = db.db.scan(start, end, read_opts(snap));
            *out = Box::into_raw(Box::new(lsm_iter {
                it,
                key: Vec::new(),
                val: Vec::new(),
            }));
            Ok(LSM_OK)
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_iter_next(
    it: *mut lsm_iter,
    key: *mut *const u8,
    key_len: *mut usize,
    val: *mut *const u8,
    val_len: *mut usize,
    errmsg: *mut *mut c_char,
) -> c_int {
    unsafe {
        guard(errmsg, || {
            if it.is_null()
                || key.is_null()
                || key_len.is_null()
                || val.is_null()
                || val_len.is_null()
            {
                return Err(invalid("iterator and output pointers must not be NULL"));
            }
            let it = &mut *it;
            match Iterator::next(&mut it.it) {
                Some(Ok((k, v))) => {
                    it.key = k;
                    it.val = v;
                    *key = it.key.as_ptr();
                    *key_len = it.key.len();
                    *val = it.val.as_ptr();
                    *val_len = it.val.len();
                    Ok(LSM_OK)
                }
                Some(Err(e)) => Err(e.into()),
                None => Ok(LSM_NOT_FOUND),
            }
        })
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_iter_free(it: *mut lsm_iter) {
    if !it.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(unsafe { Box::from_raw(it) })));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_free_buf(buf: *mut u8, len: usize) {
    if !buf.is_null() {
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(buf, len)) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lsm_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn take_err(e: *mut c_char) -> String {
        assert!(!e.is_null());
        let s = unsafe { CStr::from_ptr(e) }.to_string_lossy().into_owned();
        unsafe { lsm_free_string(e) };
        s
    }

    #[test]
    fn roundtrip_snapshot_batch_iter_in_memory() {
        unsafe {
            let mut db = ptr::null_mut();
            let mut err = ptr::null_mut();
            assert_eq!(lsm_open_in_memory(0, &mut db, &mut err), LSM_OK);
            assert_eq!(
                lsm_put(db, b"a".as_ptr(), 1, b"1".as_ptr(), 1, &mut err),
                LSM_OK
            );
            // Empty key and value via NULL + 0.
            assert_eq!(
                lsm_put(db, ptr::null(), 0, ptr::null(), 0, &mut err),
                LSM_OK
            );
            let snap = lsm_snapshot_new(db);
            assert!(!snap.is_null());
            let b = lsm_batch_new();
            assert_eq!(lsm_batch_put(b, b"b".as_ptr(), 1, b"2".as_ptr(), 1), LSM_OK);
            assert_eq!(lsm_batch_delete(b, b"a".as_ptr(), 1), LSM_OK);
            assert_eq!(lsm_write(db, b, &mut err), LSM_OK);
            lsm_batch_free(b);

            let (mut v, mut n) = (ptr::null_mut(), 0usize);
            assert_eq!(
                lsm_get(db, ptr::null(), b"a".as_ptr(), 1, &mut v, &mut n, &mut err),
                LSM_NOT_FOUND
            );
            assert!(v.is_null());
            assert_eq!(
                lsm_get(db, snap, b"a".as_ptr(), 1, &mut v, &mut n, &mut err),
                LSM_OK
            );
            assert_eq!(std::slice::from_raw_parts(v, n), b"1");
            lsm_free_buf(v, n);

            let mut it = ptr::null_mut();
            assert_eq!(
                lsm_iter_new(
                    db,
                    ptr::null(),
                    ptr::null(),
                    0,
                    ptr::null(),
                    0,
                    &mut it,
                    &mut err
                ),
                LSM_OK
            );
            let mut seen = Vec::new();
            loop {
                let (mut k, mut kl, mut vv, mut vl) = (ptr::null(), 0, ptr::null(), 0);
                match lsm_iter_next(it, &mut k, &mut kl, &mut vv, &mut vl, &mut err) {
                    LSM_OK => seen.push((
                        std::slice::from_raw_parts(k, kl).to_vec(),
                        std::slice::from_raw_parts(vv, vl).to_vec(),
                    )),
                    LSM_NOT_FOUND => break,
                    c => panic!("iter error {c}"),
                }
            }
            assert_eq!(seen, vec![(vec![], vec![]), (b"b".to_vec(), b"2".to_vec())]);
            lsm_iter_free(it);
            lsm_snapshot_free(snap);
            assert_eq!(lsm_flush(db, &mut err), LSM_OK);
            assert_eq!(lsm_compact(db, &mut err), LSM_OK);
            lsm_close(db);
        }
    }

    #[test]
    fn null_and_bad_arguments_are_errors_not_crashes() {
        unsafe {
            let mut err = ptr::null_mut();
            let (mut v, mut n) = (ptr::null_mut(), 0usize);
            assert_eq!(
                lsm_put(
                    ptr::null_mut(),
                    b"a".as_ptr(),
                    1,
                    b"1".as_ptr(),
                    1,
                    &mut err
                ),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert!(take_err(err).contains("NULL"));
            let mut db = ptr::null_mut();
            assert_eq!(lsm_open_in_memory(1, &mut db, ptr::null_mut()), LSM_OK);
            err = ptr::null_mut();
            assert_eq!(
                lsm_put(db, ptr::null(), 5, b"1".as_ptr(), 1, &mut err),
                LSM_ERR_INVALID_ARGUMENT
            );
            take_err(err);
            assert_eq!(
                lsm_get(
                    db,
                    ptr::null(),
                    b"a".as_ptr(),
                    1,
                    ptr::null_mut(),
                    &mut n,
                    ptr::null_mut()
                ),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert_eq!(
                lsm_get(
                    db,
                    ptr::null(),
                    b"a".as_ptr(),
                    usize::MAX,
                    &mut v,
                    &mut n,
                    ptr::null_mut()
                ),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert_eq!(
                lsm_open(ptr::null(), 0, &mut db, ptr::null_mut()),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert_eq!(
                lsm_batch_put(ptr::null_mut(), ptr::null(), 0, ptr::null(), 0),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert_eq!(
                lsm_write(db, ptr::null(), ptr::null_mut()),
                LSM_ERR_INVALID_ARGUMENT
            );
            let mut it = ptr::null_mut();
            assert_eq!(
                lsm_iter_new(
                    db,
                    ptr::null(),
                    ptr::null(),
                    3,
                    ptr::null(),
                    0,
                    &mut it,
                    ptr::null_mut()
                ),
                LSM_ERR_INVALID_ARGUMENT
            );
            assert!(lsm_snapshot_new(ptr::null_mut()).is_null());
            // NULL frees are no-ops.
            lsm_close(ptr::null_mut());
            lsm_batch_free(ptr::null_mut());
            lsm_snapshot_free(ptr::null_mut());
            lsm_iter_free(ptr::null_mut());
            lsm_free_buf(ptr::null_mut(), 0);
            lsm_free_string(ptr::null_mut());
            lsm_close(db);
        }
    }

    #[test]
    fn panics_do_not_cross_the_boundary() {
        unsafe {
            let mut err = ptr::null_mut();
            let code = guard(&mut err, || panic!("boom"));
            assert_eq!(code, LSM_ERR_PANIC);
            assert!(take_err(err).contains("boom"));
        }
    }
}

/// box try will box error first, and then do the same thing as try!.
#[macro_export]
macro_rules! box_try {
    ($expr:expr) => ({
        match $expr {
            Ok(r) => r,
            Err(e) => return Err(box_err!(e)),
        }
    })
}

/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<Error + Sync + Send> = ($e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

/// make a thread name with additional tag inheriting from current thread.
#[macro_export]
macro_rules! thd_name {
    ($name:expr) => ({
        $crate::util::get_tag_from_thread_name().map(|tag| {
            format!("{}::{}", $name, tag)
        }).unwrap_or_else(|| $name.to_owned())
    });
}

/// Simulating go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
#[macro_export]
macro_rules! defer {
    ($t:expr) => (
        let __ctx = $crate::util::DeferContext::new(|| $t);
    );
}

/// Log slow operations with warn!.
macro_rules! slow_log {
    ($t:expr, $($arg:tt)*) => {{
        if $t.is_slow() {
            warn!("{} [takes {:?}]", format_args!($($arg)*), $t.elapsed());
        }
    }}
}


/// Recover from panicable closure.
///
/// Please note that this macro assume the closure is able to be forced to implement `RecoverSafe`.
/// Also see https://doc.rust-lang.org/nightly/std/panic/trait.RecoverSafe.html
// Maybe we should define a recover macro too.
#[macro_export]
macro_rules! recover_safe {
    ($e:expr) => ({
        use std::panic::{AssertUnwindSafe, catch_unwind};
        use $crate::util::panic_hook;
        panic_hook::mute();
        let res = catch_unwind(AssertUnwindSafe($e));
        panic_hook::unmute();
        res
    })
}

#[macro_export]
macro_rules! map {
    () => {
        {
            use std::collections::HashMap;
            HashMap::new()
        }
    };
    ( $( $k:expr => $v:expr ),+ ) => {
        {
            use std::collections::HashMap;
            let mut temp_map = HashMap::with_capacity(count_args!($(($k, $v)),+));
            $(
                temp_map.insert($k, $v);
            )+
            temp_map
        }
    };
}


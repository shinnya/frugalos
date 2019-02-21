#[macro_export]
macro_rules! debug_wait {
    ($cond:expr, $e:expr) => {
        if cfg!(feature = "debug-wait") {
            if $cond {
                $e;
            }
        }
    };
}

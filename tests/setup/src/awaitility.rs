/// Makes given number of attempts to get the given predicate true.
/// ## Args:
/// `condition` - condition that eventually should become true
/// `attempts` - number of times to check the condition
/// `duration` - an invterway to wait between condition checks
#[macro_export]
macro_rules! await_async_for {
    ($condition: expr, $attempts: literal, $duration: expr) => {{
        let mut attempts_ = $attempts;
        loop {
            if $condition {
                break;
            };
            if attempts_ == 0 {
                panic!("No attempts left, but the condition is not satisfied");
            };
            attempts_ -= 1;
            tokio::time::sleep($duration).await;
        }
    }};
}

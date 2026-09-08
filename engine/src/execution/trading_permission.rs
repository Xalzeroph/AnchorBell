use std::future::Future;

/// Single boundary for every exchange operation that can change trading state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingPermission {
    allowed: bool,
}

impl TradingPermission {
    pub const fn new(send_orders: bool, deployment_allows_orders: bool) -> Self {
        Self {
            allowed: send_orders && deployment_allows_orders,
        }
    }

    pub const fn allowed(self) -> bool {
        self.allowed
    }

    /// A denied operation is not constructed, so it cannot accidentally touch the network.
    pub async fn execute<T, E, F, Fut>(self, operation: F) -> Result<Option<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if self.allowed {
            operation().await.map(Some)
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn denied_permission_never_invokes_the_operation() {
        for permission in [
            TradingPermission::new(false, true),
            TradingPermission::new(true, false),
            TradingPermission::new(false, false),
        ] {
            let calls = Cell::new(0);
            let result: Result<Option<()>, ()> = permission
                .execute(|| {
                    calls.set(calls.get() + 1);
                    async { Ok(()) }
                })
                .await;
            assert_eq!(result, Ok(None));
            assert_eq!(calls.get(), 0);
        }
    }

    #[tokio::test]
    async fn enabled_permission_forwards_success_and_failure() {
        assert_eq!(
            TradingPermission::new(true, true)
                .execute(|| async { Ok::<_, ()>(7) })
                .await,
            Ok(Some(7))
        );
        assert_eq!(
            TradingPermission::new(true, true)
                .execute(|| async { Err::<(), _>("rejected") })
                .await,
            Err("rejected")
        );
    }
}

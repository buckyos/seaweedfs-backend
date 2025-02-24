use std::{cell::RefCell, future::Future};
use tokio::runtime::Runtime;


thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(None);
}


pub fn with_thread_local_runtime<F: Future>(f: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // log::trace!("using existing tokio runtime");
        handle.block_on(f)
    } else {
        RUNTIME.with(|rt| {
            if rt.borrow().is_none() {
                // log::trace!("creating tokio runtime");
                *rt.borrow_mut() = Some(Runtime::new().expect("Failed to create runtime"));
            } else {
                // log::trace!("using existing created tokio runtime");
            }

            let rt_ref = rt.borrow();
            let runtime = rt_ref.as_ref().unwrap();
            runtime.block_on(f)
        })
    }
}
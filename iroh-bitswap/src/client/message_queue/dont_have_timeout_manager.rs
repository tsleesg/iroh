use ahash::AHashSet;
use cid::Cid;

#[derive(Debug, Clone)]
pub struct DontHaveTimeoutManager {}

impl DontHaveTimeoutManager {
    pub fn new() -> Self {
        todo!()
    }

    pub fn cancel_pending(&self, cancels: &AHashSet<Cid>) {
        todo!()
    }
}

use async_trait::async_trait;
use mockall::automock;

#[automock]
#[async_trait]
pub trait SequenceConsistent {}

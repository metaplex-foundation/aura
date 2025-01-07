#[cfg(test)]
mod tests {

    use interface::signature_persistence::BlockConsumer;

    use setup::rocks::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore = "already processed slots are not stored anymore"]
    async fn test_get_raw_block_on_empty_db() {}
}

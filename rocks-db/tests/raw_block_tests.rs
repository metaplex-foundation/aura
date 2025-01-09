#[cfg(test)]
mod tests {

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore = "already processed slots are not stored anymore"]
    async fn test_get_raw_block_on_empty_db() {}
}

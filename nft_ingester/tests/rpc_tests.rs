#[cfg(test)]
#[cfg(feature = "integration_tests")]
mod tests {
    use std::str::FromStr;

    use backfill_rpc::rpc::BackfillRPC;
    use entities::models::BufferedTransaction;
    use interface::solana_rpc::TransactionsGetter;
    use solana_sdk::signature::Signature;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_consume_failed_transaction_from_rpc() {
        let backfill_rpc = BackfillRPC::connect("https://api.mainnet-beta.solana.com".to_string());

        let signature = Signature::from_str("2chEzCWu5wa9bAYHa9T2j9HSC3sebbs5MKv13x2WeRqxAz87XUDQfUBrJRzrWATcyrgeugUCztwFiRqxs19NMjfN").unwrap();

        let transactions = backfill_rpc.get_txs_by_signatures(vec![signature], 1000).await.unwrap();

        assert_eq!(transactions.len(), 1);
        // TX is failed so method should return default value
        assert_eq!(transactions.get(0).unwrap(), &BufferedTransaction::default());
    }
}

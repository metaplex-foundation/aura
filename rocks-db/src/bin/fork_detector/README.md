# Fork detector

This binary is designed to detect transactions that were included in forks, specifically identifying cNFTs that were updated within these forked transactions.

The script became necessary because the previous fork cleaner could incorrectly remove data when a fork occurred. The issue arises when the same asset changes in different blocks (one of which is forked), and both blocks have different sequences. In such cases, the fork cleaner doesn't handle the database cleanup correctly. It may delete one sequence but not both, which is problematic. If the cleaner drops the sequence from the forked block (which could be higher), it will fail to backfill the lower sequence that was accepted by the majority of validators.

This binary must be run with the indexer turned off.

Once it detects a fork, it removes the sequences related to it. After that, when the indexer is relaunched, the SequenceConsistentGapFiller can identify any gaps in the sequences and fill them appropriately.


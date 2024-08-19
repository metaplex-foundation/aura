DO $$ 
BEGIN
    -- Check if the index 'fee_paid_slot_index' exists with the definition 'core_fees(fee_paid, fee_slot_updated)'
    -- That index may be created by mistake during testing
    IF EXISTS (
        SELECT 1 
        FROM pg_indexes 
        WHERE schemaname = 'public' 
            AND tablename = 'core_fees' 
            AND indexname = 'fee_paid_slot_index'
            AND indexdef = 'CREATE INDEX fee_paid_slot_index ON public.core_fees USING btree (fee_paid, fee_slot_updated)'
    ) THEN
        -- Drop the existing index
        EXECUTE 'DROP INDEX IF EXISTS fee_paid_slot_index';

        -- Create the new index with the desired definition
        EXECUTE 'CREATE INDEX fee_paid_slot_index ON public.core_fees USING btree (fee_slot_updated, fee_pubkey) WHERE NOT fee_paid';
    END IF;
END $$;

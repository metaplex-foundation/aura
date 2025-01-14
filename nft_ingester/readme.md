






## Tips

To set a global limit on request execution for PG DB, you can use the URL parameter  **statement_timeout**.

Example:
`postgres://user:password@localhost/dbname?statement_timeout=2000`

To limit only API requests use config option **api_query_max_statement_timeout_secs**


# Integration tests module

This module include integration tests for postgre-client. Those are heavy and require Docker as a dependency, so those tests are "hidden" with the `integration_tests` feature. To run all the tests, including the integration, verify you have Docker available on your machine or CI and use `cargo test --features integration_tests`.

Some tech debt left:

- the testcontainers package is of the previous version, as it includes a simple way to use Postgres container without much code, update to the recent version may require some additional work
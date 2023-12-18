FROM rust:1.70-bullseye
COPY init.sql /init.sql
COPY migration /migration
ENV INIT_FILE_PATH=/init.sql
COPY digital_asset_types /digital_asset_types
COPY rocks-db ./rocks-db
WORKDIR /migration
RUN cargo build --release
WORKDIR /migration/target/release
CMD /migration/target/release/migration up

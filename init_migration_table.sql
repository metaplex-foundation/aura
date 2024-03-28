-- this file can be removed once executed on prod

CREATE TABLE "_sqlx_migrations" (
	"version" int8 NOT NULL,
	description text NOT NULL,
	installed_on timestamptz DEFAULT now() NOT NULL,
	success bool NOT NULL,
	checksum bytea NOT NULL,
	execution_time int8 NOT NULL,
	CONSTRAINT "_sqlx_migrations_pkey" PRIMARY KEY (version)
);

INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (1, 'init', true, decode('8515E9EC03135C88EE288C6E8007608DD6DC598D15CCA1A0F45EF688F0FD4A4472555F4556BD73D6BC82A7A56F8FC0DA', 'hex'), 44544922);
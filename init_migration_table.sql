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

INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (1, 'init', true, decode('026D89ABD185043991E8047F24674F231D6AC1E4339DD5401C78BF858BD256A360C9DE2277CA921479ADFAF31DB6C3F6', 'hex'), 273155436);
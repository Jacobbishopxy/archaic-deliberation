
-- disallow table creation in the public schema

revoke create on schema public from public;

grant create on schema public to admin_user;

-- create a read-only user

create role dev_reader;

grant connect on database prod to dev_reader;

grant usage on schema public to dev_reader;

grant select on all tables in schema public to dev_reader;

alter default privileges in schema public grant select on tables to dev_reader;

create user prod_developer with password 'dev123123';

grant dev_reader to prod_developer;

-- drop a user

reassign owned by dev_reader to postgres;

drop owned by dev_reader;

drop user dev_reader;

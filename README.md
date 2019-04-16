# SMDBA

SUSE Manager Database Admin or SMDBA is a replacement for `spacewalk-dobby`.

Key features:

1. Support Oracle and PostgreSQL
2. No configuration other than just default `/etc/rhn/rhn.conf`
3. No DBI drivers at all.
4. All work is done through sudo.


In case you want to use it without SUSE Manager:

1. Change `DEFAULT_CONFIG` in class Console, which is located in 
   `$SOURCE/src/smdba/smdba` to something else.

2. By default it reacts to the following keys in the config (example):
     
       db_backend = oracle
       db_user = scott
       db_password = tiger
       db_name = test
       db_host = localhost
       db_port = 1521

3. At the moment `db_backend` can have only two values: `oracle` or `postgresql`.
   Therefore it will load `oraclegate` or `postgresqlgate` module. Once you want
   something else, like H2, you suppose to create `h2gate` module.

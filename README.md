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

## How to create a release

* Update version in setup.py and src/smdba/smdba (as an example see
https://github.com/SUSE/smdba/commit/530731003cbf3a275f7fe3bfb97b4337f975a38e

* After commiting it to the master branch, create a tag and push it:
 ```git tag X.Y.Z && git push --tags``` where X.Y.Z is the new version

* Call build-src.sh to create the tarball

* You need to update https://build.opensuse.org/package/show/systemsmanagement:Uyuni:Master/smdba, but **do not submit directly with a commit**.

* Create a Submit Request that replaces the tarball and updates the SPEC and the changelog (see https://openbuildservice.org/help/manuals/obs-user-guide/art.obs.bg.html#sec.obsbg.uc.branchprj for details)

* Prepare Submit Requests to to SUSE Manager Head and probably the released versions
  Tip: if for the Submit Request for Uyuni you told OBS not to clean the branched package when accepted, you can use that branched package for the submissions to SUSE Manager as well)



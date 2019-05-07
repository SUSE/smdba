# coding: utf-8
"""
Unit tests for essential functions in postgresql backup.
"""
from unittest.mock import MagicMock, mock_open, patch
import pytest
import smdba.postgresqlgate


class TestPgBackup:
    """
    Test suite for postgresql backup.
    """
    @patch("smdba.postgresqlgate.os.path.exists", MagicMock(return_value=False))
    def test_init_pgbackup_checks_archivecleaup(self):
        """
        Test constructor of pgbackup pg_archivecleanup installed

        :return:
        """

        with pytest.raises(Exception) as exc:
            smdba.postgresqlgate.PgBackup("/target")
        assert "The utility pg_archivecleanup was not found on the path." in str(exc)

    @patch("smdba.postgresqlgate.os.path.exists", MagicMock(return_value=True))
    def test_init_pgbackup_sets_pgdata_path(self):
        """
        Test constructor of pgbackup for pg_data is set correctly.

        :return:
        """
        target = "/some/target"
        pg_data = "/opt/pg_data"
        pgbk = smdba.postgresqlgate.PgBackup(target_path=target, pg_data=pg_data)

        assert pgbk.target_path == target
        assert pgbk.pg_data == pg_data

        pgbk = smdba.postgresqlgate.PgBackup(target_path=target)
        assert pgbk.pg_data == pgbk.DEFAULT_PG_DATA

    @patch("smdba.postgresqlgate.os.listdir", MagicMock(return_value=[]))
    @patch("smdba.postgresqlgate.stat.S_ISREG", MagicMock(return_value=True))
    def test_get_latest_restart_no_files(self):
        """
        Test latest restart filename if no files at all.

        :return:
        """
        path = "/opt/backups"
        ckp, hst, rfnm = smdba.postgresqlgate.PgBackup._get_latest_restart_filename(path=path)

        assert ckp == hst == []
        assert rfnm is None

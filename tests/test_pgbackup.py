# coding: utf-8
"""
Unit tests for essential functions in postgresql backup.
"""
import os
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
    @patch("smdba.postgresqlgate.os.stat", MagicMock())
    def test_get_latest_restart_no_files(self):
        """
        Test latest restart filename if no files at all.

        :return:
        """
        path = "/opt/backups"
        ckp, hst, rfnm = smdba.postgresqlgate.PgBackup._get_latest_restart_filename(path=path)

        assert ckp == hst == []
        assert rfnm is None

    @patch("smdba.postgresqlgate.os.listdir", MagicMock(
        return_value=[
            "0000000100000001000000AA.00000028.backup",
            "0000000100000001000000AA.000100FF.backup",
            "0000000100000001000000AA.10000000.backup",
            "0000000100000001000000AA.A00000CF.backup",
        ])
    )
    @patch("smdba.postgresqlgate.stat.S_ISREG", MagicMock(return_value=True))
    @patch("smdba.postgresqlgate.os.stat", MagicMock())
    def test_get_latest_restart_no_history(self):
        """
        Test latest restart filename. No history.

        :return:
        """
        path = "/opt/backups"
        ckp, hst, rfnm = smdba.postgresqlgate.PgBackup._get_latest_restart_filename(path=path)

        assert ckp == [
            '0000000100000001000000AA.00000028.backup',
            '0000000100000001000000AA.000100FF.backup',
            '0000000100000001000000AA.10000000.backup'
        ]
        assert hst == []
        assert rfnm == "0000000100000001000000AA.A00000CF.backup"

    @patch("smdba.postgresqlgate.os.listdir", MagicMock(
        return_value=[
            "0000000100000001000000AA.00000030.history",
            "0000000100000001000000AB.00001000.history",
            "0000000100000001000000AB.0000100A.history",
        ])
    )
    @patch("smdba.postgresqlgate.stat.S_ISREG", MagicMock(return_value=True))
    @patch("smdba.postgresqlgate.os.stat", MagicMock())
    def test_get_latest_restart_history(self):
        """
        Test latest restart filename. History should have one less file.

        :return:
        """
        path = "/opt/backups"
        ckp, hst, rfnm = smdba.postgresqlgate.PgBackup._get_latest_restart_filename(path=path)

        assert ckp == []
        assert rfnm is None
        assert hst == [
            '0000000100000001000000AA.00000030.history',
            '0000000100000001000000AB.00001000.history'
        ]

    @patch("smdba.postgresqlgate.stat.S_ISREG", MagicMock(return_value=True))
    @patch("smdba.postgresqlgate.os.stat", MagicMock())
    @patch("smdba.postgresqlgate.os.listdir", MagicMock(return_value=[]))
    def test_cleanup_backup(self):
        """
        Test cleanup backup after the process finished.

        :return:
        """
        target = "/some/target"
        pg_data = "/opt/pg_data"
        cls = smdba.postgresqlgate.PgBackup
        checkpoints = ["0000000100000001000000AA", "0000000100000001000000BB"]
        cls._get_latest_restart_filename = MagicMock(
            return_value=(checkpoints,
                          None, "0000000100000001000000AA.10000000.backup"))
        pgbk = cls(target_path=target, pg_data=pg_data)

        os_system = MagicMock()
        os_unlink = MagicMock()
        with patch("os.unlink", os_unlink) as uln, patch("os.system", os_system) as stm:
            pgbk.cleanup_backup()

        for call in os_unlink.call_args_list:
            args, kw = call
            assert args[0] == os.path.join(target, next(iter(checkpoints)))
            checkpoints.pop(0)
        assert not checkpoints

        args, kw = next(iter(os_system.call_args_list))
        assert kw == {}
        assert args[0] == "/usr/bin/pg_archivecleanup /some/target 0000000100000001000000AA.10000000.backup"

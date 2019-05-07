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
    def test_init_pkbackup_checks_archivecleaup(self):
        """
        Test constructor of pkgbackup pg_archivecleanup installed

        :return:
        """

        with pytest.raises(Exception) as exc:
            smdba.postgresqlgate.PgBackup("/target")
        assert "The utility pg_archivecleanup was not found on the path." in str(exc)

# coding: utf-8
"""
Unit tests for the base gate.
"""
from unittest.mock import MagicMock, mock_open, patch
import smdba.postgresqlgate


class TestPgGt:
    """
    Test suite for base gate.
    """
    @patch("os.path.exists", MagicMock(side_effect=[True, False, False]))
    @patch("smdba.postgresqlgate.open", new_callable=mock_open,
           read_data="key=value")
    def test_get_scenario_template(self, mck):
        """
        Gets scenario template.

        :return:
        """
        pgt = smdba.postgresqlgate.PgSQLGate({})
        template = pgt.get_scenario_template(target="psql")
        assert template == "cat - << EOF | /usr/bin/psql -t --pset footer=off\n@scenario\nEOF"

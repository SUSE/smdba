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

    @patch("os.path.exists", MagicMock(side_effect=[True, False, False]))
    @patch("smdba.postgresqlgate.open", new_callable=mock_open,
           read_data="key=value")
    def test_call_scenario(self, mck):
        """
        Calls database scenario.

        :return:
        """
        pgt = smdba.postgresqlgate.PgSQLGate({})
        pgt.get_scn = MagicMock()
        pgt.get_scn().read = MagicMock(return_value="SELECT pg_reload_conf();")
        pgt.syscall = MagicMock()

        pgt.call_scenario("pg-reload-conf.scn", target="psql")

        expectations = [
            (
                ('sudo', '-u', 'postgres', '/bin/bash'),
                {'input': 'cat - << EOF | /usr/bin/psql -t --pset footer=off\nSELECT pg_reload_conf();\nEOF'}
            )
        ]

        for call in pgt.syscall.call_args_list:
            args, kw = call
            exp_args, exp_kw = next(iter(expectations))
            expectations.pop(0)

            assert args == exp_args
            assert "input" in kw
            assert "input" in exp_kw
            assert kw["input"] == exp_kw["input"]

        assert not expectations

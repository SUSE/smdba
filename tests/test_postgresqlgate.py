# coding: utf-8
"""
Unit tests for the base gate.
"""
from unittest.mock import MagicMock, mock_open, patch
import smdba.postgresqlgate
import smdba.basegate


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

    def test_to_bytes(self):
        """
        Unicode support while converting to bytes.

        :return:
        """
        bts = smdba.basegate.BaseGate.to_bytes("спам і яйця")
        assert bts == b'\xd1\x81\xd0\xbf\xd0\xb0\xd0\xbc \xd1\x96 \xd1\x8f\xd0\xb9\xd1\x86\xd1\x8f'

    def test_to_str(self):
        """
        Unicode support while converting to string.

        :return:
        """
        sts = smdba.basegate.BaseGate.to_str(b'\xd1\x81\xd0\xbf\xd0\xb0\xd0\xbc \xd1\x96 \xd1\x8f\xd0\xb9\xd1\x86\xd1\x8f')
        assert sts == "спам і яйця"

    def test_size_pretty_round(self):
        """
        Test method for formatting human-readable sizes rounding.

        :return:
        """
        for size, res in ((0x40, "64 Bytes"), (0xff, "255 Bytes"),
                          (0x400, "1.00 KB"), (0x800, "2.00 KB"), (0x10000, "64.00 KB"),
                          (0xffff, "64.00 KB"), (0xfffff, "1024.00 KB"),
                          (0x100000, "1.00 MB"), (0x40000000, "1.00 GB"),
                          (0x10000000000, "1.00 TB"), (0x19000000000, "1.56 TB")):
            assert smdba.basegate.BaseGate.size_pretty(size=str(size)) == res

    def test_size_pretty_whitespace(self):
        """
        Test human-readable sizes formatter whitespace.

        :return:
        """
        assert smdba.basegate.BaseGate.size_pretty(size=str(0x400), no_whitespace=True) == "1.00KB"

    def test_size_pretty_intonly(self):
        """
        Test human-readable sizes formatter int only.

        :return:
        """
        assert smdba.basegate.BaseGate.size_pretty(size=str(0x19000000000), int_only=True) == "2 TB"
        assert smdba.basegate.BaseGate.size_pretty(size=str(0x19000000000), int_only=False) == "1.56 TB"

    @patch("sys.exit", new_callable=MagicMock())
    def test_to_stderr_no_data(self, ext):
        """
        Test STDERR extractor with no data
        :return:
        """
        eprint = MagicMock()
        with patch("smdba.basegate.eprint", eprint):
            out = smdba.basegate.BaseGate.to_stderr("")

        assert type(out) == bool
        assert not out
        assert not eprint.called
        assert not ext.called

    @patch("sys.exit", new_callable=MagicMock())
    def test_to_stderr_data(self, ext):
        """
        Test STDERR extractor with data

        :return:
        """
        eprint = MagicMock()
        with patch("smdba.basegate.eprint", eprint):
            out = smdba.basegate.BaseGate.to_stderr("Strike due to broken coffee machine.")

        assert out is None
        assert eprint.called
        assert ext.called

        assert ext.call_args_list[0][0][0] == 1

        expectations = [
            '\nError:\n--------------------------------------------------------------------------------',
            '  Strike due to broken coffee machine.',
            '--------------------------------------------------------------------------------'
        ]
        for call in eprint.call_args_list:
            args, kwargs = call
            assert not kwargs
            assert next(iter(expectations)) == args[0]
            expectations.pop(0)

    def test_rman_error_extraction(self):
        """
        RMAN error extraction.

        :return:
        """
        rman_log = """
RMAN-00571: ===========================================================
RMAN-00569: =============== ERROR MESSAGE STACK FOLLOWS ===============
RMAN-00571: ===========================================================
RMAN-00558: error encountered while parsing input commands
RMAN-01005: syntax error: found ")": expecting one of: "archivelog, backup, backupset, controlfilecopy, current, database, datafile, datafilecopy, (, plus, ;, tablespace"
RMAN-01007: at line 1 column 18 file: standard input
"""
        errors = smdba.basegate.BaseGate.extract_errors(rman_log)
        assert errors == ('RMAN-00558: error encountered while parsing input commands\n'
                          'RMAN-01005: syntax error: found ")": expecting one of: "archivelog,\n'
                          'backup, backupset, controlfilecopy, current, database, datafile,\n'
                          'datafilecopy, (, plus, ;, tablespace"\n'
                          'RMAN-01007: at line 1 column 18 file: standard input')

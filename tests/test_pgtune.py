# coding: utf-8
"""
Test suite for PgTune.
"""

from unittest.mock import MagicMock, patch
import pytest
import smdba.postgresqlgate


class TestPgTune:
    """
    Test PgTune class.
    """

    def test_estimate(self):
        """
        Test estimation.

        :return:
        """

        popen = MagicMock()
        popen().read = MagicMock(return_value="11.2")

        with patch("smdba.postgresqlgate.os.popen", popen):
            pgtune = smdba.postgresqlgate.PgTune(10)
            pgtune.get_total_memory = MagicMock(return_value=0x1e0384000)
            pgtune.estimate()

        assert pgtune.config['shared_buffers'] == '1920MB'
        assert pgtune.config['effective_cache_size'] == '5632MB'
        assert pgtune.config['work_mem'] == '768MB'
        assert pgtune.config['maintenance_work_mem'] == '480MB'
        assert pgtune.config['max_wal_size'] == '384MB'
        assert pgtune.config['checkpoint_completion_target'] == '0.7'
        assert pgtune.config['wal_buffers'] == '4MB'
        assert pgtune.config['constraint_exclusion'] == 'off'
        assert pgtune.config['max_connections'] == 10
        assert pgtune.config['cpu_tuple_cost'] == '0.5'

    def test_estimate_low_memory(self):
        """
        Estimation should abort unsupported low memory systems.

        :return:
        """
        popen = MagicMock()
        popen().read = MagicMock(return_value="11.2")

        with patch("smdba.postgresqlgate.os.popen", popen):
            pgtune = smdba.postgresqlgate.PgTune(10)
            pgtune.get_total_memory = MagicMock(return_value=0xfefffff)  # One byte less to 0xff00000 memory segment
            with pytest.raises(Exception) as exc:
                pgtune.estimate()
        assert "This is a low memory system and is not supported!" in str(exc)

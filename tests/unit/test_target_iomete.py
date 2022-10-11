import io
import unittest
import os
import itertools

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import patch

import singer_target_iomete


def _mock_record_to_csv_line(record):
    return record


class TestTargetIomete(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.maxDiff = None

    @patch('singer_target_iomete.flush_streams')
    @patch('singer_target_iomete.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        singer_target_iomete.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('singer_target_iomete.flush_streams')
    @patch('singer_target_iomete.DbSync')
    def test_persist_lines_with_same_schema_expect_flushing_once(self, dbSync_mock,
                                                                 flush_streams_mock):
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        singer_target_iomete.persist_lines(self.config, lines)

        self.assertEqual(1, flush_streams_mock.call_count)

    @patch('singer_target_iomete.datetime')
    @patch('singer_target_iomete.flush_streams')
    @patch('singer_target_iomete.DbSync')
    def test_persist_40_records_with_batch_wait_limit(self, dbSync_mock, flush_streams_mock, dateTime_mock):

        start_time = datetime(2021, 4, 6, 0, 0, 0)
        increment = 11
        counter = itertools.count()

        # Move time forward by {{increment}} seconds every time utcnow() is called
        dateTime_mock.utcnow.side_effect = lambda: start_time + timedelta(seconds=increment * next(counter))

        self.config['batch_size_rows'] = 100
        self.config['batch_wait_limit_seconds'] = 10
        self.config['flush_all_streams'] = True

        # Expecting 40 records
        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        singer_target_iomete.persist_lines(self.config, lines)

        # Expecting flush after every records + 1 at the end
        self.assertEqual(flush_streams_mock.call_count, 41)

    @patch('singer_target_iomete.flush_streams')
    @patch('singer_target_iomete.DbSync')
    def test_persist_lines_with_only_state_messages(self, dbSync_mock, flush_streams_mock):
        """
        Given only state messages, target should emit the last one
        """

        self.config['batch_size_rows'] = 5

        with open(f'{os.path.dirname(__file__)}/resources/streams_only_state.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # catch stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            singer_target_iomete.persist_lines(self.config, lines)

        flush_streams_mock.assert_not_called()

        print(buf.getvalue())
        self.assertEqual(
            buf.getvalue().strip(),
            '{"bookmarks": {"tap_mysql_test-test_simple_table": {"replication_key": "id", '
            '"replication_key_value": 100, "version": 1}}}')

import datetime
import json
import unittest
import os
import boto3
import singer_target_iomete

from singer_target_iomete import RecordValidationException
from singer_target_iomete.utils.exceptions import PrimaryKeyNotFoundException
from singer_target_iomete.db_sync import DbSync
from singer_target_iomete.utils.s3_upload_client import S3UploadClient

from unittest import mock

try:
    import tests.integration.utils as test_utils
except ImportError:
    import utils as test_utils

METADATA_COLUMNS = [
    '_SDC_EXTRACTED_AT',
    '_SDC_BATCHED_AT',
    '_SDC_DELETED_AT'
]


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    maxDiff = None

    def setUp(self):
        self.config = test_utils.get_test_config()
        self.iomete = DbSync(self.config)

        # Drop target schema
        if self.config['default_target_schema']:
            self.iomete.execute_query("DROP SCHEMA IF EXISTS {}".format(self.config['default_target_schema']))

        if self.config['schema_mapping']:
            for _, val in self.config['schema_mapping'].items():
                self.iomete.execute_query('drop schema if exists {}'.format(val['target_schema']))

        # Set up S3 client
        aws_access_key_id = self.config.get('aws_access_key_id')
        aws_secret_access_key = self.config.get('aws_secret_access_key')
        aws_session_token = self.config.get('aws_session_token')
        aws_session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

        self.s3_client = aws_session.client('s3',
                                            region_name=self.config.get('s3_region_name'),
                                            endpoint_url=self.config.get('s3_endpoint_url'))

    def persist_lines(self, lines):
        """Loads singer messages into iomete without table caching option"""
        singer_target_iomete.persist_lines(self.config, lines)

    def remove_metadata_columns_from_rows(self, rows):
        """Removes metadata columns from a list of rows"""
        d_rows = []
        for r in rows:
            # Copy the original row to a new dict to keep the original dict
            # and remove metadata columns
            d_row = r.copy()
            for md_c in METADATA_COLUMNS:
                d_row.pop(md_c, None)

            # Add new row without metadata columns to the new list
            d_rows.append(d_row)

        return d_rows

    def assert_metadata_columns_exist(self, rows):
        """This is a helper assertion that checks if every row in a list has metadata columns"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertTrue(md_c in r)

    def assert_metadata_columns_not_exist(self, rows):
        """This is a helper assertion that checks metadata columns don't exist in any row"""
        for r in rows:
            for md_c in METADATA_COLUMNS:
                self.assertFalse(md_c in r)

    def assert_three_streams_are_into_iomete(self, should_metadata_columns_exist=False,
                                             should_hard_deleted_rows=False):
        """
        This is a helper assertion that checks if every data from the message-with-three-streams.json
        file is available in iomete tables correctly.
        Useful to check different loading methods (unencrypted, Client-Side encryption, gzip, etc.)
        without duplicating assertions
        """
        iomete = DbSync(self.config)
        default_target_schema = self.config.get('default_target_schema', '')
        schema_mapping = self.config.get('schema_mapping', {})

        # Identify target schema name
        target_schema = None
        if default_target_schema is not None and default_target_schema.strip():
            target_schema = default_target_schema
        elif schema_mapping:
            target_schema = "tap_mysql_test"

        # Get loaded rows from tables
        table_one = iomete.execute_query(
            "SELECT * FROM {}.test_table_one ORDER BY c_pk".format(target_schema)
        )
        table_two = iomete.execute_query(
            "SELECT * FROM {}.test_table_two ORDER BY c_pk".format(target_schema)
        )
        table_three = iomete.execute_query(
            "SELECT * FROM {}.test_table_three ORDER BY c_pk".format(target_schema)
        )

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': '1'}
        ]

        table_one_without_metadata = self.remove_metadata_columns_from_rows(table_one)
        self.assertEqual(
            table_one_without_metadata, expected_table_one
        )

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = []
        if not should_hard_deleted_rows:
            expected_table_two = [
                {
                    'C_INT': 1,
                    'C_PK': 1,
                    'C_VARCHAR': '1',
                    'C_DATE': datetime.datetime(2019, 2, 1, 15, 12, 45),
                    'C_ISO_DATE': '2019-02-01'
                },
                {
                    'C_INT': 2,
                    'C_PK': 2,
                    'C_VARCHAR': '2',
                    'C_DATE': datetime.datetime(2019, 2, 10, 2, 0),
                    'C_ISO_DATE': '2019-02-10'
                }
            ]
        else:
            expected_table_two = [
                {
                    'C_INT': 2,
                    'C_PK': 2,
                    'C_VARCHAR': '2',
                    'C_DATE': datetime.datetime(2019, 2, 10, 2, 0),
                    'C_ISO_DATE': '2019-02-10'
                }
            ]

        table_two_without_metadata = self.remove_metadata_columns_from_rows(table_two)
        self.assertEqual(
            table_two_without_metadata, expected_table_two
        )

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = []
        if not should_hard_deleted_rows:
            expected_table_three = [
                {
                    'C_INT': 1,
                    'C_PK': 1,
                    'C_VARCHAR': '1',
                    'C_TIME': datetime.datetime(2019, 2, 1, 4, 0)
                },
                {
                    'C_INT': 2,
                    'C_PK': 2,
                    'C_VARCHAR': '2',
                    'C_TIME': datetime.datetime(2019, 2, 1, 7, 15)
                },
                {
                    'C_INT': 3,
                    'C_PK': 3,
                    'C_VARCHAR': '3',
                    'C_TIME': datetime.datetime(2019, 2, 1, 23, 0, 3)
                }
            ]
        else:
            expected_table_three = [
                {
                    'C_INT': 1,
                    'C_PK': 1,
                    'C_VARCHAR': '1',
                    'C_TIME': datetime.datetime(2019, 2, 1, 4, 0)
                },
                {
                    'C_INT': 2,
                    'C_PK': 2,
                    'C_VARCHAR': '2',
                    'C_TIME': datetime.datetime(2019, 2, 1, 7, 15)
                },
            ]

        table_three_without_metadata = self.remove_metadata_columns_from_rows(table_three)
        self.assertEqual(
            table_three_without_metadata, expected_table_three
        )
        # ----------------------------------------------------------------------
        # Check if metadata columns exist or not
        # ----------------------------------------------------------------------

        if should_metadata_columns_exist:
            self.assert_metadata_columns_exist(table_one)
            self.assert_metadata_columns_exist(table_two)
            self.assert_metadata_columns_exist(table_three)
        else:
            self.assert_metadata_columns_not_exist(table_one)
            self.assert_metadata_columns_not_exist(table_two)
            self.assert_metadata_columns_not_exist(table_three)

    def assert_logical_streams_are_in_iomete(self, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = iomete.execute_query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = iomete.execute_query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = iomete.execute_query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = iomete.execute_query(
            "SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(
                target_schema))

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {'CID': 1, 'CVARCHAR': "inserted row", 'CVARCHAR2': None},
            {'CID': 2, 'CVARCHAR': 'inserted row', "CVARCHAR2": "inserted row"},
            {'CID': 3, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"},
            {'CID': 4, 'CVARCHAR': "inserted row", 'CVARCHAR2': "inserted row"}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_two
        # ----------------------------------------------------------------------
        expected_table_two = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
            {'CID': 5, 'CVARCHAR': "updated row"},
            {'CID': 7, 'CVARCHAR': "updated row"},
            {'CID': 8, 'CVARCHAR': 'updated row'},
            {'CID': 9, 'CVARCHAR': "updated row"},
            {'CID': 10, 'CVARCHAR': 'updated row'}
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_three
        # ----------------------------------------------------------------------
        expected_table_three = [
            {'CID': 1, 'CVARCHAR': "updated row"},
            {'CID': 2, 'CVARCHAR': 'updated row'},
            {'CID': 3, 'CVARCHAR': "updated row"},
        ]

        # ----------------------------------------------------------------------
        # Check rows in table_four
        # ----------------------------------------------------------------------
        expected_table_four = [
            {'CID': 1, 'CTIMENTZ': None, 'CTIMETZ': None},
            {'CID': 2, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 23, 0, 15),
             'CTIMETZ': datetime.datetime(2019, 2, 1, 23, 0, 15)},
            {'CID': 3, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 12, 0, 15),
             'CTIMETZ': datetime.datetime(2019, 2, 1, 12, 0, 15)},
            {'CID': 4, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 12, 0, 15),
             'CTIMETZ': datetime.datetime(2019, 2, 1, 9, 0, 15)},
            {'CID': 5, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 12, 0, 15),
             'CTIMETZ': datetime.datetime(2019, 2, 1, 15, 0, 15)},
            {'CID': 6, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 0, 0), 'CTIMETZ': datetime.datetime(2019, 2, 1, 0, 0)},
            {'CID': 8, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 0, 0), 'CTIMETZ': datetime.datetime(2019, 2, 1, 1, 0)},
            {'CID': 9, 'CTIMENTZ': datetime.datetime(2019, 2, 1, 0, 0), 'CTIMETZ': datetime.datetime(2019, 2, 1, 0, 0)}
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_two), expected_table_two)
            self.assertEqual(self.remove_metadata_columns_from_rows(table_three), expected_table_three)
            self.assertEqual(table_four, expected_table_four)
        else:
            self.assertEqual(table_one, expected_table_one)
            self.assertEqual(table_two, expected_table_two)
            self.assertEqual(table_three, expected_table_three)
            self.assertEqual(table_four, expected_table_four)

    def assert_logical_streams_are_in_iomete_and_are_empty(self):
        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = iomete.execute_query("SELECT * FROM {}.logical1_table1 ORDER BY CID".format(target_schema))
        table_two = iomete.execute_query("SELECT * FROM {}.logical1_table2 ORDER BY CID".format(target_schema))
        table_three = iomete.execute_query("SELECT * FROM {}.logical2_table1 ORDER BY CID".format(target_schema))
        table_four = iomete.execute_query(
            "SELECT CID, CTIMENTZ, CTIMETZ FROM {}.logical1_edgydata WHERE CID IN(1,2,3,4,5,6,8,9) ORDER BY CID".format(
                target_schema))

        self.assertEqual(table_one, [])
        self.assertEqual(table_two, [])
        self.assertEqual(table_three, [])
        self.assertEqual(table_four, [])

    def assert_binary_data_are_in_iomete(self, table_name, should_metadata_columns_exist=False):
        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_one = iomete.execute_query(
            "SELECT * FROM {}.{} ORDER BY ID".format(target_schema, table_name)
        )

        # ----------------------------------------------------------------------
        # Check rows in table_one
        # ----------------------------------------------------------------------
        expected_table_one = [
            {
                "ID": "706b32",
                "DATA": "6461746132",
                "CREATED_AT": datetime.datetime(2019, 12, 17, 16, 2, 55),
            },
            {
                "ID": "706b34",
                "DATA": "6461746134",
                "CREATED_AT": datetime.datetime(2019, 12, 17, 16, 32, 22),
            },
        ]

        if should_metadata_columns_exist:
            self.assertEqual(self.remove_metadata_columns_from_rows(table_one), expected_table_one)
        else:
            self.assertEqual(table_one, expected_table_one)

    #################################
    #           TESTS               #
    #################################

    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-json.json')
        with self.assertRaises(json.decoder.JSONDecodeError):
            self.persist_lines(tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines('invalid-message-order.json')
        with self.assertRaises(Exception):
            self.persist_lines(tap_lines)

    def test_loading_tables_with_metadata_columns(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        #
        # # Turning on adding metadata columns
        self.config['add_metadata_columns'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_iomete(should_metadata_columns_exist=True)

    def test_loading_tables_with_defined_parallelism(self):
        """Loading multiple tables from the same input tap with various columns types"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')
        #
        # # Using fixed 1 thread parallelism
        self.config['parallelism'] = 1
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_iomete()

    def test_loading_tables_with_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_three_streams_are_into_iomete(
            should_metadata_columns_exist=True,
            should_hard_deleted_rows=True
        )

    def test_loading_with_multiple_schema(self):
        """Loading table with multiple SCHEMA messages"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-multi-schemas.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Check if data loaded correctly
        self.assert_three_streams_are_into_iomete(
            should_metadata_columns_exist=False,
            should_hard_deleted_rows=False
        )

    def test_loading_tables_with_binary_columns_and_hard_delete(self):
        """Loading multiple tables from the same input tap with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-binary-columns.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_iomete(
            table_name='test_binary',
            should_metadata_columns_exist=True
        )

    def test_loading_table_with_reserved_word_as_name_and_hard_delete(self):
        """Loading a table where the name is a reserved word with deleted rows"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-reserved-name-as-table-name.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        # Check if data loaded correctly and metadata columns exist
        self.assert_binary_data_are_in_iomete(
            table_name='ORDER',
            should_metadata_columns_exist=True
        )

    def test_loading_unicode_characters(self):
        """Loading unicode encoded characters"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unicode-characters.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_unicode = iomete.execute_query("SELECT * FROM {}.test_table_unicode ORDER BY C_INT".format(target_schema))

        self.assertEqual(
            table_unicode,
            [
                {'C_INT': 1, 'C_PK': 1, 'C_VARCHAR': 'Hello world, Καλημέρα κόσμε, コンニチハ'},
                {'C_INT': 2, 'C_PK': 2, 'C_VARCHAR': 'Chinese: 和毛泽东 <<重上井冈山>>. 严永欣, 一九八八年.'},
                {'C_INT': 3, 'C_PK': 3,
                 'C_VARCHAR': 'Russian: Зарегистрируйтесь сейчас на Десятую Международную Конференцию по'},
                {'C_INT': 4, 'C_PK': 4, 'C_VARCHAR': 'Thai: แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช'},
                {'C_INT': 5, 'C_PK': 5, 'C_VARCHAR': 'Arabic: لقد لعبت أنت وأصدقاؤك لمدة وحصلتم علي من إجمالي النقاط'},
                {'C_INT': 6, 'C_PK': 6, 'C_VARCHAR': 'Special Characters: [",\'!@£$%^&*()]'}
            ])

    def test_non_db_friendly_columns(self):
        """Loading non-db friendly columns like, camelcase, minus signs, etc."""
        tap_lines = test_utils.get_test_tap_lines('messages-with-non-db-friendly-columns.json')

        # Load with default settings
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        table_non_db_friendly_columns = iomete.execute_query(
            "SELECT * FROM {}.test_table_non_db_friendly_columns ORDER BY c_pk".format(target_schema))

        self.assertEqual(
            table_non_db_friendly_columns,
            [
                {'C_PK': 1, 'CAMELCASECOLUMN': 'Dummy row 1', 'MINUS-COLUMN': 'Dummy row 1'},
                {'C_PK': 2, 'CAMELCASECOLUMN': 'Dummy row 2', 'MINUS-COLUMN': 'Dummy row 2'},
                {'C_PK': 3, 'CAMELCASECOLUMN': 'Dummy row 3', 'MINUS-COLUMN': 'Dummy row 3'},
                {'C_PK': 4, 'CAMELCASECOLUMN': 'Dummy row 4', 'MINUS-COLUMN': 'Dummy row 4'},
                {'C_PK': 5, 'CAMELCASECOLUMN': 'Dummy row 5', 'MINUS-COLUMN': 'Dummy row 5'},
            ])

    def test_nested_schema_unflattening(self):
        """Loading nested JSON objects into VARIANT columns without flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Load with default settings - Flattening disabled
        self.persist_lines(tap_lines)

        # Get loaded rows from tables - Transform JSON to string at query time
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        unflattened_table = iomete.execute_query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(
                target_schema
            )
        )

        # Should be valid nested JSON strings
        self.assertEqual(
            unflattened_table,
            [{
                'C_PK': 1,
                'C_ARRAY': json.dumps([1, 2, 3]),
                'C_OBJECT': json.dumps({"key_1": "value_1"}),
                'C_OBJECT_WITH_PROPS': json.dumps({"key_1": "value_1"}),
                'C_NESTED_OBJECT': json.dumps(
                    {
                        "nested_prop_1": "nested_value_1",
                        "nested_prop_2": "nested_value_2",
                        "nested_prop_3": {
                            "multi_nested_prop_1": "multi_value_1",
                            "multi_nested_prop_2": "multi_value_2"
                        }
                    }
                )
            }]
        )

    def test_nested_schema_flattening(self):
        """Loading nested JSON objects with flattening and not not flattening"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-nested-schema.json')

        # Turning on data flattening
        self.config['data_flattening_max_level'] = 10

        # Load with default settings - Flattening disabled
        self.persist_lines(tap_lines)

        # Get loaded rows from tables
        iomete = DbSync(self.config)
        target_schema = self.config.get('default_target_schema', '')
        flattened_table = iomete.execute_query(
            "SELECT * FROM {}.test_table_nested_schema ORDER BY c_pk".format(
                target_schema
            )
        )

        # Should be flattened columns
        self.assertEqual(
            flattened_table,
            [{
                "C_PK": 1,
                "C_ARRAY": json.dumps([1, 2, 3]),
                "C_OBJECT": None,
                # Cannot map RECORD to SCHEMA. SCHEMA doesn't have properties that requires for flattening
                "C_OBJECT_WITH_PROPS__KEY_1": "value_1",
                "C_NESTED_OBJECT__NESTED_PROP_1": "nested_value_1",
                "C_NESTED_OBJECT__NESTED_PROP_2": "nested_value_2",
                "C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_1": "multi_value_1",
                "C_NESTED_OBJECT__NESTED_PROP_3__MULTI_NESTED_PROP_2": "multi_value_2",
            }]
        )

    def test_logical_streams_from_pg_with_hard_delete_and_default_batch_size_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_iomete(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_iomete(True)

    def test_logical_streams_from_pg_with_hard_delete_and_batch_size_of_5_and_no_records_should_pass(self):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams-no-records.json')

        # Turning on hard delete mode
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 5
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_iomete_and_are_empty()

    @mock.patch('singer_target_iomete.emit_state')
    def test_flush_streams_with_no_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when no intermediate flush required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size big enough to never has to flush in the middle
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.persist_lines(tap_lines)

        # State should be emitted only once with the latest received STATE message
        self.assertEqual(
            mock_emit_state.mock_calls,
            [
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                )
            ]
        )

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_iomete(True)

    @mock.patch('singer_target_iomete.emit_state')
    def test_flush_streams_with_intermediate_flushes(self, mock_emit_state):
        """Test emitting states when intermediate flushes required"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.persist_lines(tap_lines)

        # State should be emitted multiple times, updating the positions only in the stream which got flushed
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flushed edgydata until lsn: 108197216
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #2 - Flushed logical1-logical1_table2 until lsn: 108201336
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108201336,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #3 - Flushed logical1-logical1_table2 until lsn: 108237600
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108237600,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #4 - Flushed logical1-logical1_table2 until lsn: 108238768
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108238768,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #5 - Flushed logical1-logical1_table2 until lsn: 108239704,
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108239896,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108196176,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #6 - Last flush, update every stream lsn: 108240872,
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_iomete(True)

    @mock.patch('singer_target_iomete.emit_state')
    def test_flush_streams_with_intermediate_flushes_on_all_streams(self, mock_emit_state):
        """Test emitting states when intermediate flushes required and flush_all_streams is enabled"""
        mock_emit_state.get.return_value = None
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        # Set batch size small enough to trigger multiple stream flushes
        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 10
        self.config['flush_all_streams'] = True
        self.persist_lines(tap_lines)

        # State should be emitted 6 times, flushing every stream and updating every stream position
        self.assertEqual(
            mock_emit_state.call_args_list,
            [
                # Flush #1 - Flush every stream until lsn: 108197216
                mock.call(
                    {
                        "currently_syncing": None,
                        "bookmarks": {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108197216,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #2 - Flush every stream until lsn 108201336
                mock.call(
                    {
                        'currently_syncing': None,
                        'bookmarks': {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108201336,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108201336,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108201336,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108201336,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #3 - Flush every stream until lsn: 108237600
                mock.call(
                    {
                        'currently_syncing': None,
                        'bookmarks': {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108237600,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108237600,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108237600,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108237600,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #4 - Flush every stream until lsn: 108238768
                mock.call(
                    {
                        'currently_syncing': None,
                        'bookmarks': {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108238768,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108238768,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108238768,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108238768,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #5 - Flush every stream until lsn: 108239704,
                mock.call(
                    {
                        'currently_syncing': None,
                        'bookmarks': {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108239896,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108239896,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108239896,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108239896,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None},
                            "public2-wearehere": {}
                        }
                    }
                ),
                # Flush #6 - Last flush, update every stream until lsn: 108240872,
                mock.call(
                    {
                        'currently_syncing': None,
                        'bookmarks': {
                            "logical1-logical1_edgydata": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723596,
                                "xmin": None
                            },
                            "logical1-logical1_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723618,
                                "xmin": None
                            },
                            "logical1-logical1_table2": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723635,
                                "xmin": None
                            },
                            "logical2-logical2_table1": {
                                "last_replication_method": "LOG_BASED",
                                "lsn": 108240872,
                                "version": 1570922723651,
                                "xmin": None
                            },
                            "public-city": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1570922723667,
                                "replication_key_value": 4079
                            },
                            "public-country": {
                                "last_replication_method": "FULL_TABLE",
                                "version": 1570922730456,
                                "xmin": None
                            },
                            "public2-wearehere": {}
                        }
                    }
                ),
            ])

        # Every table should be loaded correctly
        self.assert_logical_streams_are_in_iomete(True)

    @mock.patch('singer_target_iomete.emit_state')
    def test_flush_streams_based_on_batch_wait_limit(self, mock_emit_state):
        """Tests logical streams from pg with inserts, updates and deletes"""
        tap_lines = test_utils.get_test_tap_lines('messages-pg-logical-streams.json')

        mock_emit_state.get.return_value = None

        self.config['hard_delete'] = True
        self.config['batch_size_rows'] = 1000
        self.config['batch_wait_limit_seconds'] = 0.1
        self.persist_lines(tap_lines)

        self.assert_logical_streams_are_in_iomete(True)
        self.assertGreater(mock_emit_state.call_count, 1, 'Expecting multiple flushes')

    def test_record_validation(self):
        """Test validating records"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-invalid-records.json')

        # Loading invalid records when record validation enabled should fail at ...
        self.config['validate_records'] = True
        with self.assertRaises(RecordValidationException):
            self.persist_lines(tap_lines)

    def test_pg_records_validation(self):
        """Test validating records from postgres tap"""
        tap_lines_invalid_records = test_utils.get_test_tap_lines('messages-pg-with-invalid-records.json')

        # Loading invalid records when record validation enabled should fail at ...
        self.config['validate_records'] = True
        with self.assertRaises(RecordValidationException):
            self.persist_lines(tap_lines_invalid_records)

        # Loading invalid records when record validation disabled, should pass without any exceptions
        self.config['validate_records'] = False
        self.persist_lines(tap_lines_invalid_records)

        # Valid records should pass for both with and without validation
        tap_lines_valid_records = test_utils.get_test_tap_lines('messages-pg-with-valid-records.json')

        self.config['validate_records'] = True
        self.persist_lines(tap_lines_valid_records)

    def test_loading_tables_with_custom_temp_dir(self):
        """Loading multiple tables from the same input tap using custom temp directory"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning on client-side encryption and load
        self.config['temp_dir'] = ('~/.pipelinewise/tmp')
        self.persist_lines(tap_lines)

        self.assert_three_streams_are_into_iomete()

    def test_aws_env_vars(self):
        """Test loading data with credentials defined in AWS environment variables
        than explicitly provided access keys"""
        try:
            # Save original config to restore later
            orig_config = self.config.copy()

            # Move aws access key and secret from config into environment variables
            os.environ['AWS_ACCESS_KEY_ID'] = os.environ.get('TARGET_IOMETE_AWS_ACCESS_KEY')
            os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ.get('TARGET_IOMETE_AWS_SECRET_ACCESS_KEY')
            del self.config['aws_access_key_id']
            del self.config['aws_secret_access_key']

            # Create a new S3 client using env vars
            s3Client = S3UploadClient(self.config)
            s3Client._create_s3_client()

        # Restore the original state to not confuse other tests
        finally:
            del os.environ['AWS_ACCESS_KEY_ID']
            del os.environ['AWS_SECRET_ACCESS_KEY']
            self.config = orig_config.copy()

    def test_loading_tables_with_no_compression(self):
        """Loading multiple tables with compression turned off"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-three-streams.json')

        # Turning off client-side encryption and load
        self.config['no_compression'] = True
        self.persist_lines(tap_lines)

        self.assert_three_streams_are_into_iomete()

    def test_parsing_date_failure(self):
        """Test if custom role can be used"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-unexpected-types.json')

        with self.assertRaises(singer_target_iomete.UnexpectedValueTypeException):
            self.persist_lines(tap_lines)

    def test_stream_with_changing_pks_should_succeed(self):
        """Test if table will have its PKs adjusted according to changes in schema key-properties"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-changing-pk.json')

        self.persist_lines(tap_lines)

        table_desc = self.iomete.execute_query(f'desc table {self.config["default_target_schema"]}.test_simple_table;')
        rows_count = self.iomete.execute_query(f'select count(1) as _count from'
                                               f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(6, rows_count[0]['_count'])

        self.assertEqual(8, len(table_desc))

        self.assertEqual('RESULTS', table_desc[1]['col_name'])
        self.assertEqual('TIME_CREATED', table_desc[2]['col_name'])
        self.assertEqual('NAME', table_desc[3]['col_name'])
        self.assertEqual('ID', table_desc[4]['col_name'])

    def test_stream_with_null_values_in_pks_should_fail(self):
        """Test if null values in PK column should abort the process"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-null-pk.json')

        with self.assertRaises(PrimaryKeyNotFoundException):
            self.persist_lines(tap_lines)

    def test_stream_with_new_pks_should_succeed(self):
        """Test if table will have new PKs after not having any"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-new-pk.json')

        self.config['primary_key_required'] = False

        self.persist_lines(tap_lines)

        table_desc = self.iomete.execute_query(f'desc table {self.config["default_target_schema"]}.test_simple_table;')
        rows_count = self.iomete.execute_query(f'select count(1) as _count from'
                                               f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(6, rows_count[0]['_count'])

        self.assertEqual(8, len(table_desc))

        self.assertEqual('RESULTS', table_desc[1]['col_name'])
        self.assertEqual('TIME_CREATED', table_desc[2]['col_name'])
        self.assertEqual('NAME', table_desc[3]['col_name'])
        self.assertEqual('ID', table_desc[4]['col_name'])

    def test_stream_with_falsy_pks_should_succeed(self):
        """Test if data will be loaded if records have falsy values"""
        tap_lines = test_utils.get_test_tap_lines('messages-with-falsy-pk-values.json')

        self.persist_lines(tap_lines)

        rows_count = self.iomete.execute_query(f'select count(1) as _count from'
                                               f' {self.config["default_target_schema"]}.test_simple_table;')

        self.assertEqual(8, rows_count[0]['_count'])

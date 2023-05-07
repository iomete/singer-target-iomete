import unittest

from unittest.mock import patch

from singer_target_iomete import db_sync


class TestDBSync(unittest.TestCase):
    """
    Unit Tests
    """

    def setUp(self):
        self.config = {}

        self.json_types = {
            'str': {"type": ["string"]},
            'str_or_null': {"type": ["string", "null"]},
            'dt': {"type": ["string"], "format": "date-time"},
            'dt_or_null': {"type": ["string", "null"], "format": "date-time"},
            'd': {"type": ["string"], "format": "date"},
            'd_or_null': {"type": ["string", "null"], "format": "date"},
            'time': {"type": ["string"], "format": "time"},
            'time_or_null': {"type": ["string", "null"], "format": "time"},
            'binary': {"type": ["string", "null"], "format": "binary"},
            'num': {"type": ["number"]},
            'int': {"type": ["integer"]},
            'int_or_str': {"type": ["integer", "string"]},
            'bool': {"type": ["boolean"]},
            'obj': {"type": ["object"]},
            'arr': {"type": ["array"]},
        }

    def test_config_validation(self):
        """Test configuration validator"""
        validator = db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'host': "dummy-value",
            'workspace_id': "dummy-value",
            'lakehouse': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'default_target_schema': "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors >= 0)
        self.assertGreater(len(validator(empty_config)), 0)

        # Minimal configuration should pass - (nr_of_errors == 0)
        self.assertEqual(len(validator(minimal_config)), 0)

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        self.assertGreater(len(validator(config_with_no_schema)), 0)

        # Configuration with schema mapping - (nr_of_errors >= 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        self.assertEqual(len(validator(config_with_schema_mapping)), 0)

        # Configuration with external stage
        config_with_external_stage = minimal_config.copy()
        config_with_external_stage['s3_bucket'] = 'dummy-value'
        config_with_external_stage['stage'] = 'dummy-value'
        self.assertEqual(len(validator(config_with_external_stage)), 0)

    def test_column_type_mapping(self):
        """Test JSON type to Snowflake column type mappings"""
        mapper = db_sync.column_type_spark

        # Snowflake column types
        sf_types = {
            'str': 'string',
            'str_or_null': 'string',
            'dt': 'timestamp',
            'dt_or_null': 'timestamp',
            'd': 'date',
            'd_or_null': 'date',
            'time': 'timestamp',
            'time_or_null': 'timestamp',
            'binary': 'string',
            'num': 'double',
            'int': 'long',
            'int_or_str': 'string',
            'bool': 'boolean',
            'obj': 'string',
            'arr': 'array',
        }

        # Mapping from JSON schema types to Snowflake column types
        for key, val in self.json_types.items():
            self.assertEqual(mapper(val), sf_types[key])

    def test_safe_column_name(self):
        self.assertEqual(db_sync.safe_column_name("columnname"), '`COLUMNNAME`')
        self.assertEqual(db_sync.safe_column_name("columnName"), '`COLUMNNAME`')
        self.assertEqual(db_sync.safe_column_name("column-name"), '`COLUMN-NAME`')
        self.assertEqual(db_sync.safe_column_name("column name"), '`COLUMN NAME`')

    @patch('singer_target_iomete.db_sync.DbSync.execute_query')
    def test_record_primary_key_string(self, query_patch):
        query_patch.return_value = [{'type': 'CSV'}]
        minimal_config = {
            'host': "dummy-value",
            'workspace_id': "dummy-value",
            'lakehouse': "dummy-value",
            'user': "dummy-value",
            'password': "dummy-value",
            'default_target_schema': "dummy-value"
        }

        stream_schema_message = {
            "stream": "public-table1",
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "c_str": {"type": ["null", "string"]},
                    "c_bool": {"type": ["boolean"]}
                }
            },
            "key_properties": ["id"]
        }

        import sys
        sys._called_from_test = True
        # Single primary key string
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123}), '123')

        # Composite primary key string
        stream_schema_message['key_properties'] = ['id', 'c_str']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 123, 'c_str': 'xyz'}), '123,xyz')

        # falsy PK field accepted
        stream_schema_message['key_properties'] = ['id']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 0, 'c_str': 'xyz'}), '0')

        # falsy PK field accepted
        stream_schema_message['key_properties'] = ['id', 'c_bool']
        dbsync = db_sync.DbSync(minimal_config, stream_schema_message)
        self.assertEqual(dbsync.record_primary_key_string({'id': 1, 'c_bool': False, 'c_str': 'xyz'}), '1,False')
        del sys._called_from_test

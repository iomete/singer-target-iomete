import unittest
import os
import gzip
import tempfile

import singer_target_iomete.file_formats.csv_format as csv


def _mock_record_to_csv_line(record, schema, data_flattening_max_level=0):
    return record


class TestCsv(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.config = {}

    def test_write_record_to_uncompressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write uncompressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate uncompressed CSV file
        with open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_write_records_to_compressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write gzip compressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with gzip.open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate gzip compressed CSV file
        with gzip.open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_record_to_csv_line(self):
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None,
        }

        schema = {
            'key1': {
                'type': ['null', 'string', 'integer'],
            },
            'key2': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date'},
                    {'type': ['null', 'string']}
                ]
            },
            'key3': {
                'type': ['null', 'string'], 'format': 'date-time',
            },
            'key4': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'time'},
                    {'type': ['null', 'string']}
                ]
            },
            'key5': {
                'type': ['null', 'string'],
            },
            'key6': {
                'type': ['null', 'string'], 'format': 'time',
            },
        }

        self.assertEqual(csv.record_to_csv_line(record, schema),
                         '"1","2030-01-22","10000-01-22 12:04:22","25:01:01","I\'m good",')

    def test_create_copy_sql(self):
        self.assertEqual(csv.create_copy_sql(table_name='foo_table',
                                             columns_no_data=["COL_4"],
                                             temporary_stage_table='temp_table',
                                             data_columns=['COL_1', 'COL_2', 'COL_3']),

                         "INSERT INTO foo_table (COL_1, COL_2, COL_3, COL_4) "
                         "SELECT COL_1, COL_2, COL_3, null FROM temp_table")

    def test_create_merge_sql(self):
        self.assertEqual(csv.create_merge_sql(table_name='foo_table',
                                              columns_no_data=["COL_4"],
                                              temporary_stage_table='temp_table',
                                              data_columns=['COL_1', 'COL_2', 'COL_3'],
                                              pk_merge_condition='s.COL_1 = t.COL_1'),

                         "MERGE INTO foo_table t USING ("
                         "SELECT COL_1, COL_2, COL_3 "
                         "FROM temp_table) s "
                         "ON s.COL_1 = t.COL_1 "
                         "WHEN MATCHED THEN UPDATE SET COL_1=s.COL_1, COL_2=s.COL_2, COL_3=s.COL_3, COL_4=null "
                         "WHEN NOT MATCHED THEN "
                         "INSERT (COL_1, COL_2, COL_3, COL_4) "
                         "VALUES (s.COL_1, s.COL_2, s.COL_3, null)")

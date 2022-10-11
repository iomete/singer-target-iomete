import sys

import time
import uuid

from singer import get_logger
from singer_target_iomete.file_formats import csv_format
from singer_target_iomete.utils import flattening, stream_utils

from singer_target_iomete.utils.exceptions import PrimaryKeyNotFoundException
from singer_target_iomete.utils.s3_upload_client import S3UploadClient
from pyhive import hive

ICEBERG_CATALOG_NAME = "spark_catalog"


def validate_config(config):
    """Validate configuration"""
    errors = []

    required_config_keys = [
        'host',
        'account_number',
        'lakehouse',
        'user',
        'password',
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(f"Required key is missing from config: [{k}]")

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    return errors


def column_type_spark(schema_property):
    """Take a specific schema property and return the spark equivalent column type"""
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'string'
    if 'object' in property_type:
        col_type = 'string'
    elif 'array' in property_type:
        col_type = 'array'
    # Every date-time JSON value is currently mapped to TIMESTAMP
    elif property_format == 'date-time':
        col_type = 'timestamp'
    elif property_format == 'date':
        col_type = 'date'
    # Every time JSON value is currently mapped to TIMESTAMP, because spark not support time DataType
    elif property_format == 'time':
        col_type = 'timestamp'
    elif property_format == 'binary':
        col_type = 'string'
    elif 'number' in property_type:
        col_type = 'double'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'string'
    elif 'integer' in property_type:
        col_type = 'long'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    return col_type


def column_type_iceberg(schema_property):
    """Take a specific schema property and return the iceberg (iomete's default provider) equivalent column type"""
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'string'
    if 'object' in property_type:
        col_type = 'string'
    elif 'array' in property_type:
        col_type = 'list'
    # Every date-time JSON value is currently mapped to TIMESTAMP
    elif property_format == 'date-time':
        col_type = 'timestamp'
    elif property_format == 'date':
        col_type = 'date'
    # Every time JSON value is currently mapped to TIMESTAMP, because spark not support time DataType
    elif property_format == 'time':
        col_type = 'timestamp'
    elif property_format == 'binary':
        col_type = 'string'
    elif 'number' in property_type:
        col_type = 'double'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'string'
    elif 'integer' in property_type:
        col_type = 'long'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    return col_type


def safe_column_name(name):
    """Generate SQL friendly column name"""
    return f'`{name}`'.upper()


def json_element_name(name):
    """Generate SQL friendly semi structured element reference name"""
    return f'`{name}`'


def column_clause_spark(name, schema_property):
    """Generate DDL column name with column type string"""
    return f'{safe_column_name(name)} {column_type_spark(schema_property)}'


def column_clause_iceberg(name, schema_property):
    """Generate DDL column name with column type string"""
    return f'{safe_column_name(name)} {column_type_iceberg(schema_property)}'


def primary_column_names(stream_schema_message):
    """Generate list of SQL friendly PK column names"""
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    """DbSync class"""

    def __init__(self, connection_config, stream_schema_message=None):
        """
            connection_config:      iomete connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into iomete.

                                    If stream_schema_message is not defined that we can use
                                    the DbSync instance as a generic purpose connection to
                                    iomete and can run individual queries. For example
                                    collecting catalog informations from iomete for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message

        # logger to be used across the class's methods
        self.logger = get_logger('target_iomete')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            self.logger.error('Invalid configuration:\n   * %s', '\n   * '.join(config_errors))
            sys.exit(1)

        self.schema_name = None

        # Init stream schema pylint: disable=line-too-long
        if self.stream_schema_message is not None:
            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #                                     not specified explicitly for a given stream in
            #                                     the `schema_mapping` object
            #   2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
            #                                     Example config.json:
            #                                           "schema_mapping": {
            #                                               "my_tap_stream_id": {
            #                                                   "target_schema": "my_iom_schema",
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_utils.stream_name_to_dict(stream_name)['schema_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception(
                    "Target schema name not defined in config. "
                    "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines "
                    f"target schema for {stream_name} stream.")

            self.data_flattening_max_level = self.connection_config.get('data_flattening_max_level', 0)
            self.flatten_schema = flattening.flatten_schema(stream_schema_message['schema'],
                                                            max_level=self.data_flattening_max_level)

        # Use external stage
        self.upload_client = S3UploadClient(connection_config)

        if hasattr(sys, '_called_from_test'):
            print("called from within a tests run")
        else:
            self.connection = self.create_connection()

    def create_connection(self):
        """Open iomete connection"""
        host = self.connection_config['host']
        account_number = self.connection_config['account_number']
        lakehouse = self.connection_config['lakehouse']
        user = self.connection_config['user']
        password = self.connection_config['password']
        database = "default"
        return hive.connect(
            host=host,
            account_number=account_number,
            lakehouse=lakehouse,
            database=database,
            username=user,
            password=password
        )

    def execute_query(self, query):
        self.logger.debug('Running query: %s', query)
        cursor = self.connection.cursor()
        cursor.execute(query)

        cols = [col[0] for col in cursor.description]
        res = []
        for row in cursor.fetchall():
            res.append({cols[i]: row[i] for i in range(len(cols))})
        return res

    def table_name(self, stream_name, is_temporary, without_schema=False):
        """Generate target table name"""
        if not stream_name:
            return None

        stream_dict = stream_utils.stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        iom_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            iom_table_name = f'{iom_table_name}_temp'

        if without_schema:
            return f'`{iom_table_name.upper()}`'

        return f'{ICEBERG_CATALOG_NAME}.{self.schema_name}.`{iom_table_name.upper()}`'

    def record_primary_key_string(self, record):
        """Generate a unique PK string in the record"""
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        flatten = flattening.flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)
        try:
            key_props = [str(flatten[p]) for p in self.stream_schema_message['key_properties']]
        except Exception as exc:
            pks = self.stream_schema_message['key_properties']
            fields = list(flatten.keys())
            raise PrimaryKeyNotFoundException(f"Cannot find {pks} primary key(s) in record. "
                                              f"Available fields: {fields}") from exc
        return ','.join(key_props)

    def put_to_stage(self, file, stream, count, temp_dir=None):
        """Upload file to s3 stage"""
        self.logger.info('Uploading %d rows to stage', count)
        return self.upload_client.upload_file(file, stream, temp_dir)

    def delete_from_stage(self, stream, s3_key):
        """Delete file from s3 stage"""
        self.logger.info('Deleting %s from stage', format(s3_key))
        self.upload_client.delete_object(s3_key)

    def create_temporary_stage_table(self, s3_key: str):
        tmp_table_name = str(uuid.uuid1()).replace("-", "_")

        columns = [
            column_clause_spark(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
        ]

        create_tbl_query = f"""
        CREATE TEMPORARY TABLE {tmp_table_name}({",".join(columns)})
        USING csv
        OPTIONS (
          header "false",
          path "s3a://{self.connection_config['s3_bucket']}/{s3_key}",
          mode "FAILFAST"
        )
        """

        self.execute_query(create_tbl_query)

        return tmp_table_name

    def load_file(self, s3_key, count, size_bytes):
        """Load a supported file type from iomete stage into target table"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        self.logger.info("Loading %d rows into '%s'", count, self.table_name(stream, False))

        table_name = self.table_name(stream, False, True)
        table_columns = [
            safe_column_name(row['COLUMN_NAME'])
            for row in self.get_table_columns(schema_name=self.schema_name, table_name=table_name)
        ]

        data_columns = [
            safe_column_name(name)
            for (name, _) in self.flatten_schema.items()
        ]

        columns_no_data = [
            col_name for col_name in table_columns
            if col_name not in data_columns
        ]

        temporary_stage_table = self.create_temporary_stage_table(s3_key)

        # Insert or Update with MERGE command if primary key defined
        if len(self.stream_schema_message['key_properties']) > 0:
            merge_sql = csv_format.create_merge_sql(table_name=self.table_name(stream, False),
                                                    columns_no_data=columns_no_data,
                                                    temporary_stage_table=temporary_stage_table,
                                                    data_columns=data_columns,
                                                    pk_merge_condition=
                                                    self.primary_key_merge_condition())

            # Get number of inserted and updated records - MERGE does insert and update
            self.execute_query(merge_sql)
            self.logger.info("Merge successfully executed for %s, size_bytes: %s",
                             self.table_name(stream, False), size_bytes)

        # Insert only in the case of no primary key
        else:
            copy_sql = csv_format.create_copy_sql(table_name=self.table_name(stream, False),
                                                  columns_no_data=columns_no_data,
                                                  temporary_stage_table=temporary_stage_table,
                                                  data_columns=data_columns)

            # Get number of inserted records - COPY does insert only
            self.execute_query(copy_sql)

            self.logger.info("Insert successfully executed for %s, size_bytes: %s",
                             self.table_name(stream, False), size_bytes)

    def primary_key_merge_condition(self):
        """Generate SQL join condition on primary keys for merge SQL statements"""
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join([f's.{c} = t.{c}' for c in names])

    def column_names(self):
        """Get list of columns in the schema"""
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self):
        """Generate CREATE TABLE SQL"""
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause_iceberg(name, schema)
            for (name, schema) in self.flatten_schema.items()
        ]

        p_table_name = self.table_name(stream_schema_message['stream'], False)
        p_columns = ', '.join(columns)
        return f"CREATE TABLE IF NOT EXISTS {p_table_name} ({p_columns})"

    def delete_rows(self, stream):
        """Hard delete rows from target table"""
        table = self.table_name(stream, False)
        query = f"DELETE FROM {table} WHERE _sdc_deleted_at IS NOT NULL"
        self.logger.info("Deleting rows from '%s' table... %s", table, query)
        self.execute_query(query)
        self.logger.info('Deleting rows finished')

    def create_schema_if_not_exists(self):
        """Create target schema if not exists"""
        schema_name = self.schema_name
        # Query realtime if not pre-collected
        schema_rows = self.execute_query(f"SHOW SCHEMAS LIKE '{schema_name.upper()}'")

        if len(schema_rows) == 0:
            query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
            self.execute_query(query)

    def table_exists(self, schema, table_name):
        table_name_without_quotes = table_name.replace("`", "")
        show_tables = self.execute_query(query=f"SHOW TABLES in {schema} LIKE '{table_name_without_quotes}'")
        return len(show_tables) > 0

    def get_table_columns(self, schema_name, table_name):
        """Get list of columns"""
        # COLUMN_NAME, DATA_TYPE
        if schema_name is None or table_name is None:
            raise Exception("Cannot get table columns. schema and/or table_name is empty")

        # return the following columns: column_name, data_type
        desc_table = self.execute_query(query=f"describe {schema_name}.{table_name}")

        table_columns = [
            {'COLUMN_NAME': row['col_name'], 'DATA_TYPE': row['data_type']}
            for row in desc_table
            if row['data_type'] is not None and len(row['data_type'].strip()) > 0
        ]

        return table_columns

    def update_columns(self):
        """Adds required but not existing columns the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        columns = self.get_table_columns(schema_name=self.schema_name, table_name=table_name)

        columns_dict = {column['COLUMN_NAME'].upper(): column for column in columns}

        columns_to_add = [
            column_clause_iceberg(name, properties_schema)
            for (name, properties_schema) in self.flatten_schema.items()
            if name.upper() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause_iceberg(name, properties_schema))
            for (name, properties_schema) in self.flatten_schema.items()
            if name.upper() in columns_dict
            and columns_dict[name.upper()]['DATA_TYPE'].upper()
            != column_type_iceberg(properties_schema).upper()
        ]

        for (column_name, column) in columns_to_replace:
            # self.drop_column(column_name, stream)
            self.version_column(column_name, stream)
            self.add_column(column, stream)

    def drop_column(self, column_name, stream):
        """Drops column from an existing table"""
        drop_column = f"ALTER TABLE {self.table_name(stream, False)} DROP COLUMN {column_name}"
        self.logger.info('Dropping column: %s', drop_column)
        self.execute_query(drop_column)

    def version_column(self, column_name, stream):
        """Versions a column in an existing table"""
        p_table_name = self.table_name(stream, False)
        p_column_name = column_name.replace("`", "")
        p_ver_time = time.strftime("%Y%m%d_%H%M")

        version_column = f"ALTER TABLE {p_table_name} RENAME COLUMN {column_name} TO `{p_column_name}_{p_ver_time}`"
        self.logger.info('Versioning column: %s', version_column)
        self.execute_query(version_column)

    def add_column(self, column, stream):
        """Adds a new column to an existing table"""
        add_column = f"ALTER TABLE {self.table_name(stream, False)} ADD COLUMN {column}"
        self.logger.info('Adding column: %s', add_column)
        self.execute_query(add_column)

    def sync_table(self):
        """Creates or alters the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        table_name_with_schema = self.table_name(stream, False)

        if self.table_exists(schema=self.schema_name, table_name=table_name):
            self.logger.info('Table %s exists', table_name_with_schema)
            self.update_columns()
        else:
            query = self.create_table_query()
            self.logger.info('Table %s does not exist. Creating...', table_name_with_schema)
            self.execute_query(query)

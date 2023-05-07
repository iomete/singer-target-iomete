import os
import json
from dotenv import load_dotenv

load_dotenv()


def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to a IOMETE instance, AWS IAM role and an S3 bucket
    # --------------------------------------------------------------------------
    # iomete instance
    config['host'] = os.environ.get('TARGET_IOMETE_HOST')
    config['workspace_id'] = os.environ.get('TARGET_IOMETE_WORKSPACE_ID')
    config['lakehouse'] = os.environ.get('TARGET_IOMETE_LAKEHOUSE')
    config['user'] = os.environ.get('TARGET_IOMETE_USER')
    config['password'] = os.environ.get('TARGET_IOMETE_PASSWORD')
    config['default_target_schema'] = os.environ.get("TARGET_IOMETE_SCHEMA")

    # AWS IAM and S3 bucket
    config['aws_access_key_id'] = os.environ.get('TARGET_IOMETE_AWS_ACCESS_KEY')
    config['aws_secret_access_key'] = os.environ.get('TARGET_IOMETE_AWS_SECRET_ACCESS_KEY')
    config['s3_bucket'] = os.environ.get('TARGET_IOMETE_S3_BUCKET')
    config['s3_key_prefix'] = os.environ.get('TARGET_IOMETE_S3_KEY_PREFIX')
    config['s3_acl'] = os.environ.get('TARGET_IOMETE_S3_ACL')

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config['disable_table_cache'] = None
    config['schema_mapping'] = None
    config['add_metadata_columns'] = None
    config['hard_delete'] = None
    config['flush_all_streams'] = None
    config['validate_records'] = None

    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines

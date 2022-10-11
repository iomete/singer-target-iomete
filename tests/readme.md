### 1. Setup environment

Move to tests directory.

```shell
virtualenv .env
source .env/bin/activate
pip install ../
```

3. To run unit tests:
```
  python3 -m pytest unit
```

4. To run integration tests: 

4.1. Set environment variables. See example 

```shell
TARGET_IOMETE_HOST=<iomete-account-number>
TARGET_IOMETE_ACCOUNT_NUMBER=<iomete-account-number>
TARGET_IOMETE_LAKEHOUSE=<iomete-lakehouse>
TARGET_IOMETE_USER=<iomete-user>
TARGET_IOMETE_PASSWORD=<iomete-password>
TARGET_IOMETE_SCHEMA=<iomete-schema>
TARGET_IOMETE_AWS_ACCESS_KEY=<aws-access-key-id>
TARGET_IOMETE_AWS_SECRET_ACCESS_KEY=<aws-access-secret-access-key>
TARGET_IOMETE_S3_BUCKET=<s3-external-bucket>
TARGET_IOMETE_S3_KEY_PREFIX=<bucket-directory>
TARGET_IOMETE_S3_ACL=<s3-target-acl>
```

4.2 Run pytests
Note: We recommend running each test individually. 
```
  python3 -m pytest test_target_iomete.py::TestIntegration::test_loading_tables_with_metadata_columns
```
After, delete created resources.









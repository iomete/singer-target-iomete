

## Setup packages

### 1. Create mysql tap environment


```shell
virtualenv .mysql
source .mysql/bin/activate
pip install --no-cache-dir pipelinewise-tap-mysql
#exit from the virtual environment
deactivate
```

### 2. Install iomete target

```shell
virtualenv .env
source .env/bin/activate
pip install .
```


## Move data from mysql to iomete

### 1. Discover source and get the source catalog 

```shell
./.mysql/bin/tap-mysql --config e2e-test/source-config.json --discover > e2e-test/generated/catalog.json
```

### 2. Edit generate catalog to select tables for replication

We need to manually edit generated catalog (`e2e-test/generated/catalog.json`) to select tables for migration. 
You could also configure other options on catalog. See: https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md

To select tables for replication, go and add the following lines to each table under the metadata object:
```
"selected": true,
"replication-method": "FULL_TABLE",
```


### 3. Test source tap

Provide the source config and edited `catalog.json`. Result will be shown in console output.

```shell
./.mysql/bin/tap-mysql --config e2e-test/source-config.json --properties e2e-test/generated/catalog.json
```

### 4. Replicate mysql to iomete

See example config [here], and check out this documentation for the config fields

```shell
./.mysql/bin/tap-mysql --config e2e-test/source-config.json --properties e2e-test/generated/catalog.json | .env/bin/singer-target-iomete --config e2e-test/iomete-config.json
```



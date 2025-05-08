# DBT SAP HANA Cloud adapter
<!-- Please include descriptive title -->
[![REUSE status](https://api.reuse.software/badge/github.com/SAP-samples/dbt-sap-hana-cloud)](https://api.reuse.software/info/github.com/SAP-samples/dbt-sap-hana-cloud)

## Description
<!-- Please include SEO-friendly description -->
The DBT SAP HANA Cloud Adapter allows seamless integration of dbt with SAP HANA Cloud, enabling data transformation, modeling, and orchestration.

## Requirements
```
python>=3.9,
dbt-core>=1.9.0,
dbt-adapters>=1.7.2,
dbt-common>=1.3.0
hdbcli>=2.22.32
```
## Python virtual environment
- The python virtual environment needs to activated before we begin the installation process
  ```
  python3 -m venv <name>
  ```
  enter the name for the virtual environment in the place holder.

- Activate the virutal environment
  ```
  source <name>/bin/activate
  ```
  use the same name which you gave above.

## Download and Installation
**Step 1.** Install dbt-sap-hana-cloud adapter
1. Clone the dbt-sap-hana-cloud repositroy
2. Navigate to the cloned repository
    > cd /path/to/dbt-sap-hana-cloud
3. for installation use the below command 
    ```
    pip3 install .
    ```

**Step 2.** Create a dbt project 
- Initialize a New dbt Project in a different location
  ```
  dbt init
  ```
  choose dbt-saphanacloud from the list and add the fields asked after the selection
**Step 3.** Profile setup (in case the dbt init command fails)

1. Edit the $HOME/.dbt/profiles.yml file within the .dbt folder (create if it does not exist).
2. Add the following configuration, replacing placeholders with your SAP HANA credentials:

#### Sample profile
```yaml
my-sap-hana-cloud-profile:
  target: dev
  outputs:
    dev:
      type: saphanacloud
      host: <host>       # SAP HANA cloud host address
      port: <port>       # Port for SAP HANA cloud
      user: <user>       # SAP HANA cloud username
      password: <password> # SAP HANA cloud password
      database: <database> # Database to connect to
      schema: <schema>   # Schema to use
      threads: <threads> # Number of threads you want to use
```

**Step 6.** Link Profile to dbt Project(in case the dbt init command fails)
- In your dbt_project.yml file (located in your dbt project folder), reference the profile name:
  ```yaml
  profile: my-sap-hana-cloud-profile
  ```
**Step 7.** Test Connection
- In the terminal, naviagte to you dbt project folder
  ```
  cd /path/to/dbt-project
  ```
- Run the following command to ensure dbt can connect to SAP HANA Cloud:
  ```
  dbt debug
  ```

## Test cases for adapter

**Step 1.** Navigate to the dbt-sap-hana-cloud repository
  ```
  cd /path/to/dbt-sap-hana-cloud
  ``` 
**Step 2.** Install Development Requirements
- In the dbt-sap-hana-cloud folder, install the dev_requirements.txt:
  ```
  pip3 install -r dev_requirements.txt
  ```
**Step 3.** Create a test.env File
- In the same folder as the adapter, create a test.env file and add the following:

  ```
  DBT_HANA_HOST=<host>       # SAP HANA Cloud host address
  DBT_HANA_PORT=<port>       # SAP HANA Cloud port
  DBT_HANA_USER=<user>       # SAP HANA Cloud username
  DBT_HANA_PASSWORD=<password> # SAP HANA Cloud password
  DBT_HANA_DATABASE=<database> # Database to connect to
  DBT_HANA_SCHEMA=<schema>   # Schema to use
  DBT_HANA_THREADS=<threads> # number of threads you want to use
  # Create 3 users in hana db with the same name as below to test grants
  DBT_TEST_USER_1= DBT_TEST_USER_1
  DBT_TEST_USER_2= DBT_TEST_USER_2
  DBT_TEST_USER_3= DBT_TEST_USER_3
  ```
**Step 4.** Test adapter functionality
- Run the following command to execute functional tests:
  ```
  python3 -m pytest tests/functional/
  ```

## DBT SAP HANA Cloud specific configuration

### Table Type
- If you want to define the type of table created for an incremental model or a table model, you can do so by adding this configuration to the model inside the `config` block.
  ```
  table_type='row'
  ```
  there are two options available for the table type either 'row' or 'column'.
> Note: The default type of table will be column if nothing is mentioned.

### Index
There are five types of indexes in the DBT SAP HANA Cloud adapter.
1. Row table
    * BTREE
    * CPBTREE
2.  Column table
    * INVERTED VALUE
    * INVERTED HASH
    * INVERTED INDIVIDUAL

Below are the example configuration to use them
- Row table
  ```
  {{
    config(
      materialized = "table",
      table_type='row',
      indexes=[
        {'columns': ['column_a'], 'type': 'BTREE'},
        {'columns': ['column_b'], 'type': 'BTREE', 'unique': True},
        {'columns': ['column_a', 'column_b'], 'type': 'CPBTREE'},
        {'columns': ['column_b', 'column_a'], 'type': 'CPBTREE', 'unique': True}
      ]
    )
  }}
  ```
- Column table
  ```
  {{
    config(
      materialized = "table",
      table_type='column',
      indexes=[
        {'columns': ['column_b'], 'type': 'INVERTED VALUE'},
        {'columns': ['column_a', 'column_b'], 'type': 'INVERTED VALUE'},
        {'columns': ['column_b', 'column_a'], 'type': 'INVERTED VALUE', 'unique': True},
        {'columns': ['column_b', 'column_c'], 'type': 'INVERTED HASH', 'unique': True},
        {'columns': ['column_a', 'column_c'], 'type': 'INVERTED INDIVIDUAL', 'unique': True}
      ]
    )
  }}
  ```
### Unique Keys as Primary key
You can now set unique keys as the primary key in an incremental and table model by simply enabling a flag. For example, you can configure it like this:


incremental model:
```
{{
  config(
    materialized = "incremental",
    unique_key = ['id', 'name', 'county'],
    unique_as_primary = true
  )
}}
```


table model:
```
{{
  config(
    materialized = "table",
    unique_key = ['id', 'name', 'county'],
    unique_as_primary = true
  )
}}
```

### Query partitions in incremental models

You can divide the transformation of an incremental model into multiple batches using the  `query_partitions` option. This wraps the SQL query in an outer query, which is then filtered based on the respective partition value.

#### Example

Model definition

```sql
{{
    config(
        materialized="incremental",
        unique_key=["ID"],
        unique_as_primary=true,
        query_partitions = [
          {
                  'column':'CATEGORY',
                  'type':'list',
                  'partitions':['train','plane','car'],
                  'default_partition_required':False
          }
        ],

    )
}}

  select 1 as ID, 'car' as CATEGORY  from sys.dummy
  union all
  select 2 as ID, 'train' as CATEGORY  from sys.dummy
  union all
  select 3 as ID, 'plane' as CATEGORY  from sys.dummy

```

Executed query for batch 1 (`CATEGORY = 'train'`)

```sql
select 
    *
from (

    select 1 as ID, 'car' as CATEGORY  from sys.dummy
    union all
    select 2 as ID, 'train' as CATEGORY  from sys.dummy
    union all
    select 3 as ID, 'plane' as CATEGORY  from sys.dummy

) t

where "CATEGORY" = 'train'
```

Executed query for batch 2 (`CATEGORY = 'plane'`)

```sql
select 
    *
from (

    select 1 as ID, 'car' as CATEGORY  from sys.dummy
    union all
    select 2 as ID, 'train' as CATEGORY  from sys.dummy
    union all
    select 3 as ID, 'plane' as CATEGORY  from sys.dummy

) t

where "CATEGORY" = 'plane'
```


Executed query for batch 3 (`CATEGORY = 'car'`)

```sql
select 
    *
from (

    select 1 as ID, 'car' as CATEGORY  from sys.dummy
    union all
    select 2 as ID, 'train' as CATEGORY  from sys.dummy
    union all
    select 3 as ID, 'plane' as CATEGORY  from sys.dummy

) t

where "CATEGORY" = 'car'
```

#### Configuration options

The `query_partitions` configuration option expects a list of `query_partitions` represented as objects. Each object has the following properties:
* **column**: The column after which the partitions (batches) are created.
* **partitions**: The definition of the partition values.
* **type**: The type of the partitions, which determines how the filter is applied. Possible variants:
    * `list`: The value must match one of the partition values exactly (e.g., `CATEGORY = 'train'`, `CATEGORY = 'car'`).
    * `range`: The partition values are sorted in ascending order. A value must be between two partition values (e.g., `CREATE_DATE >= '2023-01-01' AND CREATE_DATE < '2024-01-01'`, `CREATE_DATE >= '2024-01-01' AND CREATE_DATE < '2025-01-01'`).
* **default_partition_required**: Defines if a default partition should be added for all rows that do not match any partition value. Possible values:
    * `true`
    * `false`

> **Note:** Currently, a transformation can be partitioned after a maximum of two columns.


### Custom sqlscript materialization

Some materializations are very complicated and cannot be executed using a standard dbt materializations. Using the `sqlscript` materialization, it is possible to define custom logic with SQL Script.

Example:

```sql
{{
    config(
      materialized="sqlscript"
    )
}}

DO BEGIN

  -- Transformation written in sql script

END

```


### Automatic creation of virtual tables

dbt is intended for transforming data that already resides in a database (the "T" in an ELT process).

Since SAP HANA Cloud can access remote data using SQL (Smart Data Access (SDA) and Smart Data Integration (SDI)), dbt can also be used to extract and load data.

The `saphanacloud` dbt adapter includes a macro that automatically creates virtual tables.

To use this feature, you need to add `remote_database` and `remote_schema` as source metadata. Additionally, include the metadata value `virtual_table` with the boolean value `true`. The name of the dbt source must match the name of the remote source in SAP HANA Cloud.


Example source definition:

```yaml
version: 2

sources:
  - name: CRM
    schema: RAW_DATA
    meta: {
      virtual_table: true,
      remote_database: 'NULL',
      remote_schema: 'DEFAULT'
    }
    tables:
      - name: CUSTOMERS
      - name: SUPPLIERS
      - name: PRODUCTS
        identifier: VT_PRODUCTS
```

Then the following macro has be called:

```bash
dbt run-operation create_sources
```

This command checks if all required virtual tables exist and creates them if they do not. In the example, it will execute the following SQL statements:

```sql
CREATE VIRTUAL TABLE RAW_DATA.CUSTOMERS AT "CRM"."NULL"."DEFAULT"."CUSTOMERS";
CREATE VIRTUAL TABLE RAW_DATA.SUPPLIERS AT "CRM"."NULL"."DEFAULT"."SUPPLIERS";
CREATE VIRTUAL TABLE RAW_DATA.VT_PRODUCTS AT "CRM"."NULL"."DEFAULT"."PRODUCTS";
```
> Note: If the name of the virtual table should be different from the name of the table in the remote source, you can use the `identifier` property of the table in the source definition.


## Known Issues
<!-- You may simply state "No known issues. -->
No known issues

## How to obtain support
[Create an issue](https://github.com/SAP-samples/<repository-name>/issues) in this repository if you find a bug or have questions about the content.
 
For additional support, [ask a question in SAP Community](https://answers.sap.com/questions/ask.html).

## Contributing
If you wish to contribute code, offer fixes or improvements, please send a pull request. Due to legal reasons, contributors will be asked to accept a DCO when they create the first pull request to this project. This happens in an automated fashion during the submission process. SAP uses [the standard DCO text of the Linux Foundation](https://developercertificate.org/).

## License
Copyright (c) 2024 SAP SE or an SAP affiliate company. All rights reserved. This project is licensed under the Apache Software License, version 2.0 except as noted otherwise in the [LICENSE](LICENSE) file.


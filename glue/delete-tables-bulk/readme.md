# Script to delete AWS Glue tables in bulk
* Written in Python 3.10


## Inputs

| Name | Purpose    |
|------|------------|
| GLUE_DATA_CATALOG | Name of the Glue Catalog. Default: 'AwsDataCatalog' |
| GLUE_DB_NAME | Name of the Glue Database |
| TABLE_NAME_PATTERN | List of strings with the patterns to look for in the table names |

## Limitations

This script uses AWS API's [Glue GetTables](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTables.html) which has a hard limit of 100 results. To overcome this, the same pattern can be added multiple times to the list of patterns.

## Pseudocode

1. Traverse the list TABLE_NAME_PATTERN
2. For each pattern: get a list of tables that match the pattern
3. Traverse the list of tables returned
4. For each table: invoke glue.delete_table()

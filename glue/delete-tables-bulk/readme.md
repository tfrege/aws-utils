# Script to delete AWS Glue tables in bulk
* Written in Python 3.10
* Gets a list of patterns to look for in the table names, and deletes the tables that match them


## Inputs

| Name | Purpose    |
|------|------------|
| GLUE_DATA_CATALOG | Name of the Glue Catalog. Default: 'AwsDataCatalog' |
| GLUE_DB_NAME | Name of the Glue Database |
| TABLE_NAME_PATTERNS | List of strings with the patterns to look for in the table names |

## Requirements
* Python 3.10+
* If running this in AWS Lambda: a timeout of at least 5 min
* A role with appropiate permissions (see the file iam_policy.json)
* :exclamation: Logging enabled (CloudWatch log or alternative... you're deleting tables! Make sure to leave a trace if you need to do an autopsy later :))

## Limitations

This script uses AWS API's [Glue GetTables](https://docs.aws.amazon.com/glue/latest/webapi/API_GetTables.html) which has a hard limit of 100 results. To overcome this, the same pattern can be added multiple times to TABLE_NAME_PATTERNS.

## Pseudocode

1. Traverse the list TABLE_NAME_PATTERNS
2. For each pattern: get a list of tables that match it
3. Traverse the list of tables returned
4. For each table: invoke glue.delete_table()

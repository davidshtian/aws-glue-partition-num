import boto3
from prettytable import PrettyTable

res = PrettyTable()
res.field_names = ["Database", "Table", "Partition Num"]

glue = boto3.client('glue')

dbs = glue.get_databases()['DatabaseList']


def depaginate(function, resource_key, **kwargs):
    # Will depaginate results made to an aws client
    response = function(**kwargs)
    results = response[resource_key]
    while (response.get("NextToken", None) is not None):
        response = function(NextToken=response.get("NextToken"), **kwargs)
        results = results + response[resource_key]
    return results


for db in dbs:
    db_name = db['Name']
    tables = glue.get_tables(DatabaseName=db_name)['TableList']
    for table in tables:
        table_name = table['Name']
        partitions = depaginate(glue.get_partitions, 'Partitions',
                                DatabaseName=db_name, TableName=table_name, MaxResults=100)
        res.add_row([db_name, table_name, len(partitions)])

print(res)

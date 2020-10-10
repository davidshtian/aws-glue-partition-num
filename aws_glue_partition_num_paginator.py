import boto3
from prettytable import PrettyTable

res = PrettyTable()
res.field_names = ["Database", "Table", "Partition Num"]

glue = boto3.client('glue')

dbs = glue.get_databases()['DatabaseList']

for db in dbs:
    db_name = db['Name']
    tables = glue.get_tables(DatabaseName=db_name)['TableList']
    for table in tables:
        table_name = table['Name']
        paginator = glue.get_paginator('get_partitions')
        partitions_info = paginator.paginate(DatabaseName=db_name, TableName=table_name, PaginationConfig={
            'MaxItems': 100000, 'PageSize': 1000})
        partitions = [
            partition for partition_info in partitions_info for partition in partition_info['Partitions']]
        res.add_row([db_name, table_name, len(partitions)])

print(res)

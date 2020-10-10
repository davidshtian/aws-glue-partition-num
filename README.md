# AWS Glue Partition Numbers
Get the numbers of AWS Glue partitions of each table in databases.

Thanks for the 
[lukeplausin/aws-depaginate-boto.py](https://gist.github.com/lukeplausin/a3670cd8f115a783a822aa0094015781) to add the paginator feature. 

Otherwise, you could try to use the version leveraging [Glue.Paginator.GetPartitions](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Paginator.GetPartitions).

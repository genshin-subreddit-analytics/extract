import boto3

class DatabaseHelper:
    """
    Composer for All Data Class
    """

    def __init__(
            self, 
            subreddit_name,
            table_name,
            pk_name,
            last_archived_attr_name
        ):
        self.subreddit = subreddit_name
        self.dynamodb = boto3.resource('dynamodb')
        self.table_name = table_name
        self.db_table = self.dynamodb.Table(table_name)
        self.db_pk_name = pk_name
        self.db_last_archived_attr_name = last_archived_attr_name

    def get_last_archived_time(self) -> int:
        response = self.db_table.get_item(
            Key={
                self.db_pk_name: self.subreddit
            }
        )

        if 'Item' in response:
            last_time = response['Item'][self.db_last_archived_attr_name]
            return int(last_time)
        else:
            return 0

    def set_last_archived_time(self, time_ : int):
        self.db_table.update_item(
            Key={
                self.db_pk_name: self.subreddit
            },
            UpdateExpression=f"SET {self.db_last_archived_attr_name} = :new_value",
            ExpressionAttributeValues={
                ':new_value': time_
            }
        )

import os
import boto3

class DatabaseHelper:
    """
    Composer for All Data Class
    """

    def __init__(self, subreddit_name):
        self.subreddit = subreddit_name
        self.dynamodb = boto3.resource('dynamodb')
        self.db_table = self.dynamodb.Table(os.environ["DB_TABLE_NAME"])
        self.db_pk_name = os.environ["DB_TABLE_PRIMARY_KEY"]
        self.db_last_archived_attr_name = os.environ["DB_LAST_ARCHIVED_ATTR_NAME"]

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

import os
import boto3
import pytz
import datetime

from .data_extractor import DataExtractor

class DataManager:
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
    
    def set_time(self, last_time: int, curr_time: int) -> None:
        self.curr_time = curr_time
        self.last_time = last_time

    def get_comments(self):
        # Get Data
        self.comment_archiver = DataExtractor(
                    self.subreddit,
                    self.last_time,
                    self.curr_time
                )
        self.comment_archiver.get_comments()
        # Clean Deleted and Removed Comments
        self.comment_archiver.clean()
        # Build DataFrame
        self.comment_df = self.comment_archiver.get_df()
        print(self.comment_df)
        return self.comment_df
    
    @classmethod
    def unix_epoch_to_tz(cls, unix_timestamp, timezone):
        dt_utc = datetime.datetime.utcfromtimestamp(unix_timestamp)
        tz = pytz.timezone(timezone)
        return dt_utc.replace(tzinfo=pytz.utc).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S %Z')

import os

class EnvironmentHelper:
    SUBREDDIT_NAME = os.environ["SUBREDDIT_NAME"]
    CLIENT_TZ = os.environ["CLIENT_TZ"]
    DB_TABLE_NAME = os.environ["DB_TABLE_NAME"]
    DB_TABLE_PRIMARY_KEY = os.environ["DB_TABLE_PRIMARY_KEY"]
    DB_LAST_ARCHIVED_ATTR_NAME = os.environ["DB_LAST_ARCHIVED_ATTR_NAME"]
    BUCKET_NAME = os.environ["BUCKET_NAME"]

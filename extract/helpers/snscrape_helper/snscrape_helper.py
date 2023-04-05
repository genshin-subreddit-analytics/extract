import pytz
import datetime
import logging
import pandas as pd
from snscrape.modules.reddit import RedditSubredditScraper

logger = logging.getLogger(__name__)

class SnscrapeHelper:
    """
    Get Subreddit Data
    """

    def __init__(self, subreddit, after, before):
        self.genshin_comments_list = []
        self.scraper = RedditSubredditScraper(
                subreddit,
                submissions=False,
                comments=True,
                after=after,
                before=before,
                retries=17
            )

    def get_comments(self, batch_size = 100000):
        data = []
        for idx, comment in enumerate(self.scraper.get_items()):
            data.append({
                "ID": comment.id,
                "Date": comment.date,
                "Author": comment.author,
                "URL": comment.url,
                "Body": comment.body,
                "Parent ID": comment.parentId
            })
            if idx % batch_size == 0:
                logger.info(f"COMMENT COUNTER #{idx}")
                df = pd.DataFrame(data).astype({
                    "ID": "object",
                    "Date": "datetime64[ns, UTC]",
                    "Author": "object",
                    "URL": "object",
                    "Body": "object",
                    "Parent ID": "object"
                })
                yield df
                data = []
        if data:
            df = pd.DataFrame(data).astype({
                "ID": "object",
                "Date": "datetime64[ns, UTC]",
                "Author": "object",
                "URL": "object",
                "Body": "object",
                "Parent ID": "object"
            })
            yield df

    @staticmethod
    def clean(comment_df):
        excluded_comments = (
            (comment_df["Body"] == "[deleted]") |
            (comment_df["Body"] == "[removed]") |
            (comment_df["Author"] == "AutoModerator")
        )

        cleaned_df = comment_df[~excluded_comments]
        return cleaned_df

    @staticmethod
    def convert_unix_to_timezone_time(unix_timestamp, timezone_name):
        # Convert Unix timestamp to a Python datetime object
        datetime_obj = datetime.datetime.fromtimestamp(unix_timestamp)

        # Get the pytz timezone object
        timezone = pytz.timezone(timezone_name)

        # Convert datetime object to the desired timezone
        datetime_obj = timezone.localize(datetime_obj)

        return datetime_obj.strftime('%Y-%m-%d %H:%M:%S %Z')

    @staticmethod
    def convert_isoformat_to_unix_timestamp(date_string):
        date_obj = datetime.datetime.fromisoformat(date_string)
        unix_timestamp = int(date_obj.timestamp())
        return unix_timestamp

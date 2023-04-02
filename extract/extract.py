import traceback
import os
import time

from module.email_helper import EmailHelper
from module.database_helper import DatabaseHelper
from module.snscrape_helper import SnscrapeHelper


def main():
    # Get Status (from Database? S3?)
    subreddit = os.environ["SUBREDDIT_NAME"]
    client_tz = os.environ["CLIENT_TZ"]
    # Get AWS Helpers
    email_helper = EmailHelper()
    database_helper = DatabaseHelper(subreddit)
    # Get Time
    start_time = database_helper.get_last_archived_time()
    end_time = int(time.time())
    # Initialize snscrape helper
    snscrape_helper = SnscrapeHelper(subreddit, start_time, end_time)
    try:  # Acquiring, Cleaning, and Storing Data
        # Send email template: extraction starting
        email_helper.send(
            "GSA-Start",
            {
                "subreddit": subreddit,
                "start_time": SnscrapeHelper.unix_epoch_to_tz(start_time, client_tz),
                "end_time": SnscrapeHelper.unix_epoch_to_tz(end_time, client_tz)
            }
        )
        # Get Data
        comment_df = snscrape_helper.get_comments().clean().get_df()
        print(comment_df)
        # Write DataFrame to S3
    except Exception as e:  # Handle Failure
        # Send email: extraction failed, why failed
        stacktrace = traceback.format_exc()
        email_helper.send(
            "GSA-Error",
            {
                "subreddit": subreddit,
                "start_time": SnscrapeHelper.unix_epoch_to_tz(start_time, client_tz),
                "end_time": SnscrapeHelper.unix_epoch_to_tz(end_time, client_tz),
                "error_trace": stacktrace
            }
        )
        print(e)
    else:  # Handle Success
        # Send email: extraction succesful, num of data parsed
        email_helper.send(
            "GSA-Complete",
            {
                "subreddit": subreddit,
                "start_time": SnscrapeHelper.unix_epoch_to_tz(start_time, client_tz),
                "end_time": SnscrapeHelper.unix_epoch_to_tz(end_time, client_tz)
            }
        )


if __name__ == "__main__":
    main()

import traceback
import time

from helpers import EmailHelper, DatabaseHelper, SnscrapeHelper, EnvironmentHelper, StorageHelper


def main():
    # Get Status (from Database? S3?)
    subreddit = EnvironmentHelper.SUBREDDIT_NAME
    client_tz = EnvironmentHelper.CLIENT_TZ
    # Get AWS Helpers
    email_helper = EmailHelper()
    database_helper = DatabaseHelper(
            subreddit,
            EnvironmentHelper.DB_TABLE_NAME,
            EnvironmentHelper.DB_TABLE_PRIMARY_KEY,
            EnvironmentHelper.DB_LAST_ARCHIVED_ATTR_NAME
        )
    storage_helper = StorageHelper(EnvironmentHelper.BUCKET_NAME)
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
        # Write DataFrame to S3
        storage_helper.upload_dataframe(comment_df, f"{subreddit}-{end_time}")
        # Update Last Archived Time
        database_helper.set_last_archived_time(end_time)
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

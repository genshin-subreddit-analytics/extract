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
                "start_time": SnscrapeHelper.convert_unix_to_timezone_time(start_time, client_tz),
                "end_time": SnscrapeHelper.convert_unix_to_timezone_time(end_time, client_tz),
            }
        )
        
        batch_curr_time = end_time
        for comment_df in snscrape_helper.get_comments(batch_size=500_000):
            batch_last_time = SnscrapeHelper.convert_isoformat_to_unix_timestamp(
                str(comment_df["Date"].iloc[-1])
            )
            file_name = f"{subreddit}---{batch_last_time}-{batch_curr_time}.parquet"
            StorageHelper.save_to_disk_parquet(
                comment_df, 
                file_name
            )
            storage_helper.upload_to_cloud(file_name)

            batch_curr_time = batch_last_time

        # Update Last Archived Time
        database_helper.set_last_archived_time(end_time)
    except Exception as _:  # Handle Failure
        # Send email: extraction failed, why failed
        stacktrace = traceback.format_exc()
        email_helper.send(
            "GSA-Error",
            {
                "subreddit": subreddit,
                "start_time": SnscrapeHelper.convert_unix_to_timezone_time(start_time, client_tz),
                "end_time": SnscrapeHelper.convert_unix_to_timezone_time(end_time, client_tz),
                "error_trace": stacktrace
            }
        )
    else:  # Handle Success
        # Send email: extraction succesful, num of data parsed
        email_helper.send(
            "GSA-Complete",
            {
                "subreddit": subreddit,
                "start_time": SnscrapeHelper.convert_unix_to_timezone_time(start_time, client_tz),
                "end_time": SnscrapeHelper.convert_unix_to_timezone_time(end_time, client_tz),
            }
        )


if __name__ == "__main__":
    main()

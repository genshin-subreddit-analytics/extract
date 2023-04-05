import traceback
import datetime
import logging

from helpers import EmailHelper, DatabaseHelper, SnscrapeHelper, EnvironmentHelper, StorageHelper

logger = logging.getLogger(__name__)

def main():
    # Get Status from environment
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
    end_time = int(datetime.datetime.now().timestamp())
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
            # Get earliest time in DataFrame
            batch_last_time = SnscrapeHelper.convert_isoformat_to_unix_timestamp(
                str(comment_df["Date"].iloc[-1])
            )
            # Construct File Name
            file_last_time = datetime.datetime.fromtimestamp(batch_last_time).isoformat().replace(':', '_')
            file_curr_time = datetime.datetime.fromtimestamp(batch_curr_time).isoformat().replace(':', '_')
            file_name = f"{subreddit}-comments-{file_last_time}_to_{file_curr_time}.parquet"
            # Save to Disk
            StorageHelper.save_to_disk_parquet(
                comment_df, 
                file_name
            )
            # Delete DataFrame
            del comment_df
            # Upload to Cloud
            logger.info(f"Uploading {file_name} to Cloud Storage")
            storage_helper.upload_to_cloud(file_name)

            # Update oldest time in DataFrame
            batch_curr_time = batch_last_time

        # Update Last Archived Time
        logger.info(f"Updating r/{subreddit}'s last archived time to {end_time}")
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

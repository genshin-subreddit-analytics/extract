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
    storage_helper.set_name(f"{subreddit}-{end_time}")
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

        # Save Pandas Dataframe into Disk per Batch Size
        storage_helper.save_to_disk(
            # Comment Generator
            snscrape_helper.get_comments,
            # Batch Size
            100000,
            # Dataframe Cleaner
            SnscrapeHelper.clean
        )
        # Upload previously saved Dataframe (csv) into Cloud
        storage_helper.upload_to_cloud()
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

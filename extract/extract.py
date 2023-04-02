import traceback
import os
import time

from module.email_manager import EmailManager
from module.data_manager import DataManager


def main():
    # Get Status (from Database? S3?)
    subreddit = os.environ["SUBREDDIT_NAME"]
    client_tz = os.environ["CLIENT_TZ"]
    # Get AWS Clients
    email_manager = EmailManager()
    data_manager = DataManager(subreddit)
    # Get Time
    start_time = data_manager.get_last_archived_time()
    end_time = int(time.time())
    try:  # Acquiring, Cleaning, and Storing Data
        # Send email template: extraction starting
        email_manager.send(
            "GSA-Start",
            {
                "subreddit": subreddit,
                "start_time": DataManager.unix_epoch_to_tz(start_time, client_tz),
                "end_time": DataManager.unix_epoch_to_tz(end_time, client_tz)
            }
        )
        # Get Data
        data_manager.set_time(start_time, end_time)
        df = data_manager.get_comments()
        print(df)
        # Write DataFrame to S3
    except Exception as e:  # Handle Failure
        # Send email: extraction failed, why failed
        stacktrace = traceback.format_exc()
        email_manager.send(
            "GSA-Error",
            {
                "subreddit": subreddit,
                "start_time": DataManager.unix_epoch_to_tz(start_time, client_tz),
                "end_time": DataManager.unix_epoch_to_tz(end_time, client_tz),
                "error_trace": stacktrace
            }
        )
        print(e)
    else:  # Handle Success
        # Send email: extraction succesful, num of data parsed
        email_manager.send(
            "GSA-Complete",
            {
                "subreddit": subreddit,
                "start_time": DataManager.unix_epoch_to_tz(start_time, client_tz),
                "end_time": DataManager.unix_epoch_to_tz(end_time, client_tz),
            }
        )


if __name__ == "__main__":
    main()

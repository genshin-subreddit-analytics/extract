import traceback
import os
from datetime import datetime

from module.email_manager import EmailManager


def main():
    try:  # Acquiring, Cleaning, and Storing Data
        # Get SES Client
        email_manager = EmailManager()
        # Get Status (from Database? S3?)
        subreddit = os.environ["SUBREDDIT_NAME"]
        start_time = "-"
        end_time = datetime.now()
        # Send email template: extraction starting
        email_manager.send(
            "GSA-Start",
            {
                "subreddit": subreddit,
                "start_time": start_time,
                "end_time": end_time.strftime("%c")
            }
        )
        # Get Data
        # Build DataFrame
        # Clean DataFrame
        # Write DataFrame to S3
    except Exception as e:  # Handle Failure
        # Send email: extraction failed, why failed
        stacktrace = traceback.format_exc()
        email_manager.send(
            "GSA-Error",
            {
                "subreddit": subreddit,
                "start_time": start_time,
                "end_time": end_time.strftime("%c"),
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
                "start_time": start_time,
                "end_time": end_time.strftime("%c")
            }
        )


if __name__ == "__main__":
    main()

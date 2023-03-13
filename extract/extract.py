from datetime import datetime

from module.email_manager import EmailManager


def main():
    try:  # Acquiring, Cleaning, and Storing Data
        # Get Status (from Database? S3?)
        end_time = datetime.now()
        # Send email: extraction starting
        email_manager = EmailManager()
        email_manager.send(
            "GSA-Start",
            {
                "subreddit": "niloumains",
                "start_time": "-",
                "end_time": end_time.strftime("%c")
            }
        )
        # Get Data
        # Build DataFrame
        # Clean DataFrame
        # Write DataFrame to S3
    except:  # Handle Failure
        # Send email: extraction failed, why failed
        pass
    else:  # Handle Success
        # Send email: extraction succesful, num of data parsed
        email_manager.send(
            "GSA-Complete",
            {
                "subreddit": "niloumains",
                "start_time": "June 26th, 2003",
                "end_time": end_time.strftime("%c")
            }
        )


if __name__ == "__main__":
    main()

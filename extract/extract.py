import time

from GenshinImpactComments import GenshinImpactComments
from FileHandler import FileHandler


def main():

    curr_epoch_time = int(time.time())

    subreddit_comments = GenshinImpactComments()
    subreddit_comments.get_comments()
    subreddit_comments.clean()

    file_handler = FileHandler(
            time=curr_epoch_time,
            output_folder="out",
            log_folder="log"
        )
    file_handler.write_output_to_csv(subreddit_comments.get_comments())
    file_handler.write_to_log()


if __name__ == "__main__":
    main()

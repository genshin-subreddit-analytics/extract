import pandas as pd
from snscrape.modules.reddit import RedditSubredditScraper


class GenshinImpactComments:
    def __init__(self, curr_epoch_time):
        self.genshin_comments_list = []
        self.scraper = RedditSubredditScraper(
                "Genshin_Impact",
                submissions=False,
                comments=True,
                before=curr_epoch_time
            )

    def get_comments(self):
        for idx, comment in enumerate(self.scraper.get_items()):
            if idx % 100 == 0:
                print(f"Gathered {idx} comments")
            if idx >= 100:
                break
            self.genshin_comments_list.append([
                comment.id,
                comment.date,
                comment.author,
                comment.url,
                comment.body,
                comment.parentId
            ])

        self.genshin_comments = pd.DataFrame(
                self.genshin_comments_list,
                columns=[
                    "ID",
                    "Date",
                    "Author",
                    "URL",
                    "Body",
                    "Parent ID"
                    ]
            )

    def clean(self):
        deleted_comments = self.genshin_comments["Body"] == "[deleted]"
        removed_comments = self.genshin_comments["Body"] == "[removed]"
        spam_comments = self.genshin_comments["Author"] == "AutoModerator"

        print(f"{sum(deleted_comments)} comment(s) deleted")
        print(f"{sum(removed_comments)} comment(s) removed")
        print(f"{sum(spam_comments)} comment(s) from AutoModerator")

        self.genshin_comments = self.genshin_comments[
                ~deleted_comments
            ]
        self.genshin_comments = self.genshin_comments[
                ~removed_comments
            ]
        self.genshin_comments = self.genshin_comments[
                ~spam_comments
            ]

    def get_df(self):
        return self.genshin_comments


def main():
    curr_epoch_time = int(time.time())
    genshin_comments = []
    SubredditScraper = RedditSubredditScraper(
            "Genshin_Impact",
            submissions=False,
            comments=True,
            before=curr_epoch_time
        )

    num_of_comments = 50000
    print(f"Gathering {num_of_comments} tweets before {curr_epoch_time}")
    for idx, comment in enumerate(SubredditScraper.get_items()):
        if idx >= num_of_comments:
            break
        genshin_comments.append([
            comment.id,
            comment.date,
            comment.author,
            comment.url,
            comment.body,
            comment.parentId
        ])

    comments_df = pd.DataFrame(
            genshin_comments,
            columns=[
                "ID",
                "Date",
                "Author",
                "URL",
                "Body",
                "Parent ID"
                ]
        )

    num_deleted = sum(comments_df["Body"] == "[deleted]")
    num_removed = sum(comments_df["Body"] == "[removed]")
    num_bots = sum(comments_df["Author"] == "AutoModerator")

    print(f"{num_deleted} comments are [deleted]")
    print(f"{num_removed} comments are [removed]")
    print(f"{num_bots} Comments from AutoModerator")


if __name__ == "__main__":
    main()


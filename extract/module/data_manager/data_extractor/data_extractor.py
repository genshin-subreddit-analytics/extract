import pandas as pd
from snscrape.modules.reddit import RedditSubredditScraper

class DataExtractor:
    """
    Get Subreddit Data
    """

    def __init__(self, subreddit, last_epoch_time, curr_epoch_time):
        self.genshin_comments_list = []
        self.scraper = RedditSubredditScraper(
                subreddit,
                submissions=False,
                comments=True,
                after=last_epoch_time,
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

        # print(f" \
        #       {len(self.genshin_comments)}, \
        #       {len(deleted_comments)}, \
        #       {len(removed_comments)}, \
        #       {len(spam_comments)}")

        self.genshin_comments = self.genshin_comments[
                ~deleted_comments.reindex(self.genshin_comments.index)
            ]
        self.genshin_comments = self.genshin_comments[
                ~removed_comments.reindex(self.genshin_comments.index)
            ]
        self.genshin_comments = self.genshin_comments[
                ~spam_comments.reindex(self.genshin_comments.index)
            ]

    def get_df(self):
        return self.genshin_comments

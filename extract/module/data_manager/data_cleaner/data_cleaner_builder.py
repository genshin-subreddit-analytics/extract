from data_cleaner import DataCleaner


class DataCleanerBuilder:
    """
    Builder class for DataCleaner
    """

    def __init__(self):
        self.data_cleaner: DataCleaner = DataCleaner()
        return self

    def build(self):
        return self.data_cleaner

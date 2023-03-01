from data_extractor import DataExtractor


class DataExtractorBuilder:
    """
    Builder class for DataCleaner
    """

    def __init__(self):
        self.data_extractor: DataExtractor = DataExtractor()
        return self

    def build(self):
        return self.data_cleaner

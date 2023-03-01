import os


class FileHandler:
    def __init__(
        self,
        time,
        output_folder,
        log_folder
    ):
        self.time = time
        self.output_folder = output_folder
        self.log_folder = log_folder

    def write_output_to_csv(self, df):
        filename = f"./{self.output_folder}/{self.time}.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, sep='\t')

    def write_to_log(self, timestamp):
        filename = f"./{self.log_folder}/timestamps.log"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "a") as timestamp_file:
            timestamp_file.write(f"{self.time}\n")

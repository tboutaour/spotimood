from argparse import ArgumentParser
import datetime


class Arguments:
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    def __init__(self):
        self.parser = ArgumentParser()
        self.parser.add_argument("--start-datetime", help="Start day with YYYY-MM-DD HH:mm:SS format.")
        self.parser.add_argument("--end-datetime", help="End day with YYYY-MM-DD HH:mm:SS format.")

    def get_start_datetime(self):
        args = self.parser.parse_args()
        return datetime.datetime.strptime(args.start_datetime, self.DATETIME_FORMAT)

    def get_end_datetime(self):
        args = self.parser.parse_args()
        return datetime.datetime.strptime(args.end_datetime, self.DATETIME_FORMAT)

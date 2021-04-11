from argparse import ArgumentParser
import datetime


class Arguments:
    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self):
        self.parser = ArgumentParser()
        self.parser.add_argument("--start-day", help="Start day with YYYY-MM-DD format.")
        self.parser.add_argument("--end-day", help="End day with YYYY-MM-DD format.")

    def get_start_date(self):
        args = self.parser.parse_args()
        return datetime.datetime.strptime(args.start_day, self.DATE_FORMAT)

    def get_end_date(self):
        args = self.parser.parse_args()
        return datetime.datetime.strptime(args.end_day, self.DATE_FORMAT)

import boto3
import argparse
import time


# request objects

# process JSON request

# call different function depending on request

# implement create function

# two different functions, one to make all data into a string and put it in S3 bucket
# one to put data in DynmoDB 

# add logging

# create unit tests

class Consumer:
    def __init__():
        pass

    def run(self):
        while True:
            if self.request():
                pass
            else:
                time.sleep(0.1)


def initialize_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request", help="where to get requests")
    parser.add_argument("-wb", "--store_s3", help="where to store requests in s3 bucket")
    parser.add_argument("-dwt", "--store_db", help="name of dynamodb table")
    return parser

if __name__ == "__main__":
    parser = initialize_parser()
    args = parser.parse_args()
    if args.request and args.store_s3:
        consumer = Consumer(args.request, args.store_s3)
        consumer.run()
    elif args.request and args.store_db:
        consumer = Consumer(args.request, args.store_db)
        consumer.run()


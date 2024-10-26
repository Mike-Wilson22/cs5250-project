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

class S3_Requester():
    def __init__(self, bucket_name):
        self.s3 = boto3.resouce('s3')
        self.request_bucket = self.s3.Bucket(bucket_name)

    def retrieve(self):
        pass

class Consumer:
    def __init__(self, request, store):
        self.request = request
        self.store = store

    def run(self):
        try:
            while True:
                if self.request():
                    pass
                else:
                    time.sleep(0.1)
        except KeyboardInterrupt:
            exit(0)

    def request_data(self):
        return self.request.retrieve()

    def create(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass

class S3_Consumer(Consumer):
    def __init__(self, request, store):
        super(self, S3_Requester(request), store)
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(store)

    def request_data(self):
        


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
        consumer = Consumer(args.request, args.store_s3, 0)
        consumer.run()
    elif args.request and args.store_db:
        consumer = Consumer(args.request, args.store_db, 1)
        consumer.run()


import boto3
import argparse
import time
import json


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
        self.request_bucket = bucket_name

    def retrieve(self):
        s3 = boto3.client('s3')
        objects = s3.get_paginator('list_objects_v2')
        pages = objects.paginate(Bucket=self.request_bucket, PaginationConfig={'MaxItems': 1})

        for page in pages:
            key = page['Contents'][0]['Key']
            object = s3.get_object(Bucket=self.request_bucket, Key=key)
            # s3.delete_object(Bucket=self.request_bucket, Key=key)
            return json.loads(object['Body'].read().decode('utf-8'))
        
        return False

        

class Consumer:
    def __init__(self, request, store):
        self.request = request
        self.store = store

    def run(self):
        try:
            x = True
            while x:
                if self.request_data():
                    print(self.data)
                    x = False
                else:
                    time.sleep(0.1)
    
        except KeyboardInterrupt:
            exit(0)

    def request_data(self):
        self.data = self.request.retrieve()
        return self.data

    def create(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass

class S3_Consumer(Consumer):
    def __init__(self, request, store):
        super(S3_Consumer, self).__init__(S3_Requester(request), store)
        self.s3 = boto3.resource('s3')

    def create(self):
        pass


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
        consumer = S3_Consumer(args.request, args.store_s3)
        consumer.run()
    elif args.request and args.store_db:
        consumer = S3_Consumer(args.request, args.store_db)
        consumer.run()


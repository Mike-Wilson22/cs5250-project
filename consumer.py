import boto3
import argparse
import time
import json
import logging


# implement create function

# two different functions, one to make all data into a string and put it in S3 bucket
# one to put data in DynmoDB 

# add logging

# create unit tests

class S3_Requester():
    def __init__(self, bucket_name, logger):
        self.request_bucket = bucket_name
        self.logger = logger
        self.s3 = boto3.client('s3')

    def retrieve(self):
        objects = self.s3.get_paginator('list_objects_v2')
        pages = objects.paginate(Bucket=self.request_bucket, PaginationConfig={'MaxItems': 1})

        for page in pages:
            if 'Contents' in page:
                key = page['Contents'][0]['Key']
                object = self.s3.get_object(Bucket=self.request_bucket, Key=key)
                self.logger.info("Request Retriever - Retrieved Request")
                self.s3.delete_object(Bucket=self.request_bucket, Key=key)
                self.logger.info("Request Retriever - Deleted Request")
                return json.loads(object['Body'].read().decode('utf-8'))
            return False

        

class Consumer:
    def __init__(self, request, store, logger):
        self.request = request
        self.store = store
        self.logger = logger

    def run(self):
        try:
            while True:
                if self.request_data():
                    self.call_correct_function()
                else:
                    time.sleep(0.1)
        except KeyboardInterrupt:
            exit(0)

    def call_correct_function(self):
        if self.data['type'] == 'create':
            self.create()
        elif self.data['type'] == 'delete':
            self.delete()
        elif self.data['type'] == 'update':
            self.update()

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
    def __init__(self, request, store, logger):
        super(S3_Consumer, self).__init__(S3_Requester(request, logger), store, logger)
        self.s3 = boto3.client('s3')

    def create(self):
        del self.data['type']
        del self.data['requestId']
        owner_name = self.data['owner'].lower().replace(" ", "-")
        key = f"widgets/{owner_name}/{self.data['widgetId']}"
        self.s3.put_object(Bucket=self.store, Body=str(self.data), Key=key)
        self.logger.info("Widget Creater - Created Widget in S3 Bucket")

    def delete(self):
        #TODO: implement delete method
        pass

    def update(self):
        #TODO: implement update method
        pass


class DB_Consumer(Consumer):
    def __init__(self, request, store, logger):
        super(DB_Consumer, self).__init__(S3_Requester(request, logger), store, logger)
        db = boto3.resource('dynamodb')
        self.table = db.Table(self.store)

    def create(self):
        del self.data['type']
        del self.data['requestId']
        self.data['id'] = self.data['widgetId']
        del self.data['widgetId']

        self.data = self.transform(self.data)
        self.table.put_item(TableName=self.store, Item=self.data)
        self.logger.info("Widget Creater - Created Widget in DynamoDB")

    def delete(self):
        #TODO: implement delete method
        pass

    def update(self):
        #TODO: implement update method
        pass

    def transform(self, dictionary):
        for object in dictionary['otherAttributes']:
            dictionary[object['name']] = object['value']
        del dictionary['otherAttributes']
        return dictionary


def initialize_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request", help="where to get requests")
    parser.add_argument("-wb", "--store_s3", help="where to store requests in s3 bucket")
    parser.add_argument("-dwt", "--store_db", help="name of dynamodb table")
    return parser

def initialize_logger():
    logging.basicConfig(filename='consumer.log', level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S')
    return logging.getLogger(__name__)

if __name__ == "__main__":
    logger = initialize_logger()
    parser = initialize_parser()
    args = parser.parse_args()
    if args.request and args.store_s3:
        consumer = S3_Consumer(args.request, args.store_s3, logger)
        consumer.run()
    elif args.request and args.store_db:
        consumer = DB_Consumer(args.request, args.store_db, logger)
        consumer.run()


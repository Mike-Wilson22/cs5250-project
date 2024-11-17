import boto3
import argparse
import time
import json
import logging
import botocore
import sys


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

class SQS_Requester():
    def __init__(self, queue_name, logger):
        self.queue_name = queue_name
        self.logger = logger
        self.sqs = boto3.client('sqs')
        self.messages = []

    def retrieve(self):
        if len(self.messages) == 0:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_name,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=10,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=10,
                WaitTimeSeconds=0
            )
            
            if 'Messages' in response.keys():
                self.messages = response['Messages']
                self.logger.info(f"Request Retriever - Retrieved {len(self.messages)} Requests")

                message = self.messages.pop()
                receipt_handle = message['ReceiptHandle']

                # Delete received message from queue
                self.sqs.delete_message(
                    QueueUrl=self.queue_name,
                    ReceiptHandle=receipt_handle
                )
                self.logger.info("Request Retriever - Deleted Request")

                return json.loads(message['Body'])
            return False

        else:
            message = self.messages.pop()
            receipt_handle = message['ReceiptHandle']

            # Delete received message from queue
            self.sqs.delete_message(
                QueueUrl=self.queue_name,
                ReceiptHandle=receipt_handle
            )
            self.logger.info("Request Retriever - Deleted Request")

            return json.loads(message['Body'])

class Consumer:
    def __init__(self, request, store, logger, timeout):
        self.request = request
        self.store = store
        self.logger = logger
        self.time = time.time()
        self.timeout = timeout

    def run(self):
        try:
            while time.time() - self.time < self.timeout or self.timeout == -1:
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
    def __init__(self, requester, store, logger, timeout):
        super(S3_Consumer, self).__init__(requester, store, logger, timeout)
        self.s3 = boto3.client('s3')

    def create(self):
        del self.data['type']
        del self.data['requestId']
        owner_name = self.data['owner'].lower().replace(" ", "-")
        key = f"widgets/{owner_name}/{self.data['widgetId']}"
        self.s3.put_object(Bucket=self.store, Body=str(self.data), Key=key)
        self.logger.info("Widget Processor - Created Widget in S3 Bucket")

    def delete(self):
        owner_name = self.data['owner'].lower().replace(" ", "-")
        key = f"widgets/{owner_name}/{self.data['widgetId']}"
        try:
            self.s3.head_object(Bucket=self.store, Key=key)
            self.s3.delete_object(Bucket=self.store, Key=key)
            self.logger.info("Widget Processor - Deleted Widget from S3 Bucket")

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.logger.warning("Widget Processor - Delete Request Failed, Widget Does Not Exist")
            else:
                exit(1)

    def update(self):
        owner_name = self.data['owner'].lower().replace(" ", "-")
        key = f"widgets/{owner_name}/{self.data['widgetId']}"
        try:
            self.s3.head_object(Bucket=self.store, Key=key)
            str_object = self.s3.get_object(Bucket=self.store, Key=key)
            dict_object = json.loads(str_object['Body'].read().decode('utf-8').replace("'", "\""))
            len_attr = len(dict_object['otherAttributes'])
            for item_new in self.data['otherAttributes']:
                changed = False
                for item_old in range(0, len_attr):
                    if dict_object['otherAttributes'][item_old]['name'] == item_new['name'] and item_new['value'] != None:
                        dict_object['otherAttributes'][item_old]['value'] = item_new['value']
                        changed = True
                if not changed:
                    if item_new['value'] != None:
                        dict_object['otherAttributes'].append({"name" : item_new['name'], "value" : item_new['value']})

            for new_key in self.data.keys():
                if new_key not in ["otherAttributes", "widgetId", "owner", "type", "requestId"] and self.data[new_key] != '':
                    dict_object[new_key] = self.data[new_key]

            self.s3.put_object(Bucket=self.store, Body=str(dict_object), Key=key)
            self.logger.info("Widget Processor - Updated Widget in S3 Bucket")

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.logger.warning("Widget Processor - Update Request Failed, Widget Does Not Exist")
            else:
                exit(1)


class DB_Consumer(Consumer):
    def __init__(self, requester, store, logger, timeout):
        super(DB_Consumer, self).__init__(requester, store, logger, timeout)
        db = boto3.resource('dynamodb')
        self.table = db.Table(self.store)

    def create(self):
        del self.data['type']
        del self.data['requestId']
        self.data['id'] = self.data['widgetId']
        del self.data['widgetId']

        self.data = self.transform(self.data)
        self.table.put_item(TableName=self.store, Item=self.data)
        self.logger.info("Widget Processor - Created Widget in DynamoDB")

    def delete(self):
        check_item = self.table.get_item(Key={'id' : self.data['widgetId']})
        if 'Item' in check_item:
            self.table.delete_item(Key={'id': self.data['widgetId']})
            self.logger.info("Widget Processor - Deleted Widget in DynamoDB")
        else:
            self.logger.warning("Widget Processor - Delete Request Failed, Widget Does Not Exist")

    def update(self):
        check_item = self.table.get_item(Key={'id' : self.data['widgetId']})
        if 'Item' in check_item:
            update_expr = "SET "
            expr_attr_values = {}
            expr_attr_names = {}
            for item in self.data['otherAttributes']:
                if item['value'] != '' and item['name'] in ['height', 'width', 'color', 'rating']:
                    update_expr += f"#{item['name']} = :{item['name']}, "
                    expr_attr_names[f"#{item['name']}"] = item['name']
                    expr_attr_values[f":{item['name']}"] = item['value']

            for key in self.data.keys():
                if key not in ["otherAttributes", "widgetId", "owner", "type", "requestId"] and self.data[key] != '':
                    update_expr += f"#{key} = :{key}, "
                    expr_attr_names[f"#{key}"] = key
                    expr_attr_values[f":{key}"] = self.data[key]

            if len(update_expr) > 5:  # Check if update_expr has more than just "SET "
                update_expr = update_expr[:-2]  # Remove last two characters (", ")

            # Perform the update
            self.table.update_item(
                Key={'id' : self.data['widgetId']},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values
            )

            self.logger.info("Widget Processor - Updated Widget in DynamoDB")
        else:
            self.logger.warning("Widget Processor - Update Request Failed, Widget Does Not Exist")

    def transform(self, dictionary):
        for object in dictionary['otherAttributes']:
            dictionary[object['name']] = object['value']
        del dictionary['otherAttributes']
        return dictionary


def initialize_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-rb", "--request_s3", help="where to get requests (S3 bucket)")
    parser.add_argument("-wb", "--store_s3", help="where to store requests in s3 bucket")
    parser.add_argument("-dwt", "--store_db", help="name of dynamodb table")
    parser.add_argument("-sqs", "--request_sqs", help="where to get requests (SQS queue)")
    return parser

def initialize_logger():
    logging.basicConfig(filename='consumer.log', level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    return logger

if __name__ == "__main__":
    logger = initialize_logger()
    parser = initialize_parser()
    args = parser.parse_args()
    timeout = -1
    if args.store_s3:
        if args.request_s3:
            consumer = S3_Consumer(S3_Requester(args.request_s3, logger), args.store_s3, logger, timeout)
        else:
            consumer = S3_Consumer(SQS_Requester(args.request_sqs, logger), args.store_s3, logger, timeout)
    elif args.store_db:
        if args.request_s3:
            consumer = DB_Consumer(S3_Requester(args.request_s3, logger), args.store_db, logger, timeout)
        else:
            consumer = DB_Consumer(SQS_Requester(args.request_sqs, logger), args.store_db, logger, timeout)

    consumer.run()


# SQS url: https://sqs.us-east-1.amazonaws.com/785484577828/cs5250-requests
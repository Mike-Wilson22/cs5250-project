from consumer import Consumer, S3_Consumer, S3_Requester, DB_Consumer, SQS_Requester, initialize_logger, initialize_parser
import json
import botocore

class mock_logger:
    def __init__(self):
        pass
    def info(self, message):
        self.message = message
    def warning(self, message):
        self.message = message

class mock_sqs:
    def __init__(self, data):
        self.data = data
    def receive_message(self, QueueUrl=None, AttributeNames=None, MaxNumberOfMessages=None, MessageAttributeNames=None, VisibilityTimeout=None, WaitTimeSeconds=None):
        return self.data
    def delete_message(self, QueueUrl=None, ReceiptHandle=None):
        self.receipt = ReceiptHandle

class mock_s3:
    def __init__(self, data):
        self.data = data
    def get_paginator(self, arg1):
        return mock_paginator(self.data)
    def get_object(self, Bucket="", Key=""):
        self.bucket = Bucket
        self.key= Key
        return {"Body" : mock_body(self.body)}
    def delete_object(self, Bucket="", Key=""):
        if Bucket == self.bucket and Key == self.key:
            self.good = True
        else:
            self.good = False
    def put_object(self, Bucket, Body, Key):
        self.new_message = Body
        parts = Key.split("/")
        assert parts[0] == "widgets"
        assert parts[1].replace("-", " ") in Body.lower()
        assert parts[2] in Body
        assert type(Body) == type("")
    def head_object(self, Bucket, Key):
        self.new_message = "{\"description\" : \"None\"}"
        if self.key != Key:
            raise botocore.exceptions.ClientError({'Error' : {'Code' : "404"}}, {'Error' : {'Code' : "404"}})
    def set_key(self, key):
        self.key = key
    def set_body(self, body):
        self.body = body

class mock_paginator:
    def __init__(self, data):
        self.data = data
    def paginate(self, Bucket="", PaginationConfig=""):
        return(self.data)

class mock_body:
    def __init__(self, data):
        self.data = data
    def read(self):
        return self.data.encode('utf-8')
    
class mock_table:
    def __init__(self, data):
        self.data = data
    def put_item(self, TableName, Item):
        assert TableName == self.data
    def get_item(self, Key):
        self.key = Key
        return self.data
    def delete_item(self, Key):
        self.key = Key
    def update_item(self, Key, UpdateExpression=None,ExpressionAttributeNames=None,ExpressionAttributeValues=None):
        self.update = UpdateExpression
        self.key = Key
        self.expr_names= ExpressionAttributeNames
        self.expr_values = ExpressionAttributeValues


def test_initialize_parser():
    parser = initialize_parser()
    action_list = ['help', 'request_s3', 'store_s3', 'store_db', 'request_sqs']
    for i in range(0, 4):
        assert action_list[i] == parser._actions[i].dest

def test_initialize_logger():
    logger = initialize_logger()
    assert logger.name == "consumer"
    assert logger.level == 0
    
def test_consumer_correct_function(monkeypatch):
    def mock_create():
        assert consumer.data == {'type' : 'create'}
    def mock_delete():
        assert consumer.data == {'type' : 'delete'}
    def mock_update():
        assert consumer.data == {'type' : 'update'}
    consumer = Consumer(None, None, None, -1)
    monkeypatch.setattr(consumer, 'create', mock_create)
    monkeypatch.setattr(consumer, 'delete', mock_delete)
    monkeypatch.setattr(consumer, 'update', mock_update)
    consumer.data = {'type' : 'create'}
    consumer.call_correct_function()
    consumer.data = {'type' : 'delete'}
    consumer.call_correct_function()
    consumer.data = {'type' : 'update'}
    consumer.call_correct_function()

def test_s3_requester_retrieve():
    requester = S3_Requester(None, mock_logger())
    requester.s3 = mock_s3([{}])
    assert requester.retrieve() == False
    requester.s3 = mock_s3([{'Contents' : [{'Key' : 1}]}])
    requester.s3.set_body('{"data" : "good"}')
    assert requester.retrieve() == {"data" : "good"}
    assert requester.s3.good == True

def test_sqs_requester_retrieve():
    requester = SQS_Requester("None", mock_logger())
    requester.sqs = mock_sqs({'yay': "nothing", "nothing2" : "there is nothing"})
    assert requester.retrieve() == False
    requester.sqs = mock_sqs({"nothing" : "this means nothing", "Messages" : ["", "", "", {'ReceiptHandle' : "something", "Body" : "{\"nothing\" : \"yay\"}"}]})
    assert requester.retrieve() == {'nothing' : 'yay'}
    assert len(requester.messages) == 3
    assert requester.sqs.receipt == "something"

def test_s3_consumer_create():
    consumer = S3_Consumer(None, "store", mock_logger(), -1)
    consumer.s3 = mock_s3({})
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9'}
    consumer.create()
    assert 'type' not in consumer.data
    assert 'requestId' not in consumer.data
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9'
                     'otherAtt'}
    
def test_s3_consumer_delete():
    consumer = S3_Consumer(None, "store", mock_logger(), -1)
    consumer.s3 = mock_s3({})
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9'}
    consumer.s3.set_key('matt-num')
    consumer.delete()
    assert consumer.s3.key == 'matt-num'
    consumer.s3.set_key('nothing')
    consumer.delete()
    assert consumer.logger.message == "Widget Processor - Delete Request Failed, Widget Does Not Exist"

def test_s3_consumer_update():
    consumer = S3_Consumer(None, "store", mock_logger(), -1)
    consumer.s3 = mock_s3({})
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9', "description" : "nummy2", "label" : "", "otherAttributes" : [{"name" : "width", "value" : "11"}, {"name" : "height", "value" : None}, {"name" : "color", "value" : "red"}]}
    consumer.s3.set_key("nothing")
    consumer.update()
    assert consumer.logger.message == "Widget Processor - Update Request Failed, Widget Does Not Exist"
    consumer.s3.set_key("widgets/matt-num/y19x9")
    consumer.s3.set_body('{"owner" : "MATT NUM", "widgetId" : "y19x9", "description" : "nummy", "label" : "something", "otherAttributes" : [{"name" : "width", "value" : "10"}, {"name" : "height", "value" : "10"}, {"name" : "rating", "value" : "2.0"}]}')
    consumer.update()
    new_message = json.loads(consumer.s3.new_message.replace("'", "\""))
    assert new_message['description'] == "nummy2"
    assert new_message['label'] == "something"
    assert new_message['otherAttributes'][0] == {"name" : 'width', 'value' : '11'}
    assert new_message['otherAttributes'][1] == {"name" : 'height', 'value' : '10'}
    assert new_message['otherAttributes'][2] == {"name" : 'rating', 'value' : '2.0'}
    assert new_message['otherAttributes'][3] == {"name" : 'color', 'value' : 'red'}


def test_db_consumer_create():
    consumer= DB_Consumer(None, "store", mock_logger(), -1)
    consumer.table = mock_table("store")
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9',
                     'otherAttributes' : [{'name': '1', 'value' : '2'}, {'name' : '3', 'value' : '4'}]}
    consumer.create()
    assert 'type' not in consumer.data
    assert 'requestId' not in consumer.data
    assert 'widgetId' not in consumer.data
    assert consumer.data['id'] == 'y19x9'
    assert consumer.data['1'] == '2'
    assert consumer.data['3'] == '4'

def test_db_consumer_delete():
    consumer= DB_Consumer(None, "store", mock_logger(), -1)
    consumer.table = mock_table({})
    consumer.data = {"owner" : "MATT NUM", "widgetId" : "y19x9", "description" : "nummy", "label" : "something", "otherAttributes" : [{"name" : "width", "value" : "10"}, {"name" : "height", "value" : "10"}, {"name" : "rating", "value" : "2.0"}]}
    consumer.delete()
    assert consumer.logger.message == "Widget Processor - Delete Request Failed, Widget Does Not Exist"
    consumer.table = mock_table({'Item' : ''})
    consumer.delete()
    assert consumer.table.key == {"id" : "y19x9"}

def test_db_consumer_update():
    consumer= DB_Consumer(None, "store", mock_logger(), -1)
    consumer.table = mock_table({})
    consumer.data = {"owner" : "MATT NUM", "widgetId" : "y19x9", "description" : "nummy", "label" : "something", "otherAttributes" : [{"name" : "width", "value" : "10"}, {"name" : "height", "value" : "10"}, {"name" : "rating", "value" : "2.0"}, {"name" : "width-unit", "value" : "10"}]}
    consumer.update()
    assert consumer.logger.message == "Widget Processor - Update Request Failed, Widget Does Not Exist"
    consumer.table = mock_table({'Item' : ''})
    consumer.update()
    assert consumer.table.key == {"id" : "y19x9"}
    assert consumer.table.expr_names == {'#width': 'width', '#height': 'height', '#rating': 'rating', '#description': 'description', '#label': 'label'}
    assert consumer.table.expr_values == {':width': '10', ':height': '10', ':rating': '2.0', ':description': 'nummy', ':label': 'something'}
    assert consumer.table.update == "SET #width = :width, #height = :height, #rating = :rating, #description = :description, #label = :label"
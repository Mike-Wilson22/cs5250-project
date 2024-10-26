from consumer import Consumer, S3_Consumer, S3_Requester, DB_Consumer, initialize_logger, initialize_parser
import json

class mock_logger:
    def __init__(self):
        pass
    def info(self, message):
        pass
class mock_s3:
    def __init__(self, data):
        self.data = data
    def get_paginator(self, arg1):
        return mock_paginator(self.data)
    def get_object(self, Bucket="", Key=""):
        self.bucket = Bucket
        self.key= Key
        return {"Body" : mock_body('{"data" : "good"}')}
    def delete_object(self, Bucket="", Key=""):
        if Bucket == self.bucket and Key == self.key:
            self.good = True
        else:
            self.good = False
    def put_object(self, Bucket, Body, Key):
        parts = Key.split("/")
        assert parts[0] == "widgets"
        assert parts[1].replace("-", " ") in Body.lower()
        assert parts[2] in Body
        assert type(Body) == type("")

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


def test_initialize_parser():
    parser = initialize_parser()
    action_list = ['help', 'request', 'store_s3', 'store_db']
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
    consumer = Consumer(None, None, None)
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
    assert requester.retrieve() == {"data" : "good"}
    assert requester.s3.good == True

def test_s3_consumer_create():
    consumer = S3_Consumer(None, "store", mock_logger())
    consumer.s3 = mock_s3({})
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9'}
    consumer.create()
    assert 'type' not in consumer.data
    assert 'requestId' not in consumer.data
    consumer.data = {'type' : None, 'requestId' : None, 'owner' : 'MATT NUM', 'widgetId' : 'y19x9'
                     'otherAtt'}

def test_db_consumer_create():
    consumer= DB_Consumer(None, "store", mock_logger())
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
    
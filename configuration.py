from flask import Flask
from flask_restx import Api
import boto3
import time

global db, app, api, times

def measure_time(func):
    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        times[func.__name__] = end - start
        # print(func.__name__, end - start)
        return result

    return wrap

def init():
    global times
    times = {}

    # Initialize firestore
    global db
    db = boto3.resource('dynamodb', region_name='eu-west-3',
                        aws_access_key_id='AKIAY63XXK7JV6COPNS2',
                        aws_secret_access_key='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'
                        )

    # Initialize Flask App
    global app, api
    app = Flask(__name__)
    api = Api(app=app, version="1.0", title="Softlab-Project Cloud Run DynamoDB APIs")

    return app

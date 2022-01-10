import datetime

from flask import request, jsonify
from flask_restx import Resource, fields, abort
from random import randrange
from decimal import Decimal
import json

import boto3
from modules import dynamodb

import configuration

def dumper(obj):
    try:
        return obj.toJSON()
    except AttributeError:
        return float(obj)

class EnergyData(Resource):

    @configuration.measure_time
    def post(self):

        # Initialize global times keeper
        configuration.times = {}

        # Get parameters
        payload = request.json
        zone_codes = payload['zone_codes']
        date_from = payload['date_from']
        duration = payload['duration']
        join = payload['join']
        light = payload['light']

        # Get data
        data = dynamodb.get_energy_data(zone_codes, date_from, duration, join, light)

        # Make them JSON serializable
        for i in range(len(data)):
            doc = data[i]

            for key in doc.keys():
                if isinstance(doc[key], Decimal):
                    data[i][key] = float(doc[key])

            if join:

                # For reference zones
                for key in doc['ReferenceZoneInfo'].keys():
                    if isinstance(doc['ReferenceZoneInfo'][key], Decimal):
                        data[i]['ReferenceZoneInfo'][key] = float(doc['ReferenceZoneInfo'][key])

                # For resolution code
                data[i]['ResolutionCodeInfo']['Id'] = float(doc['ResolutionCodeInfo']['Id'])

        return {
            'times': configuration.times,
            'parameters': payload,
            'len_of_data': len(data),
            'data': data
        }

    @configuration.measure_time
    def get(self):

        # Initialize global times keeper
        configuration.times = {}

        # Get query parameters
        zone_code = request.args.get('zone_code', default=None, type=str)
        date_from = request.args.get('date_from', default='2020-01-01 00:00:00', type=str)
        duration = request.args.get('duration', default='10', type=str)

        if zone_code is None:
            abort(400, f"Zone Code needed.", statusCode=400)

        # Get data
        data = dynamodb.get_energy_data(int(zone_code), date_from, int(duration), False, True)

        # Make them JSON serializable
        for i in range(len(data)):
            doc = data[i]
            for key in doc.keys():
                if isinstance(doc[key], Decimal):
                    data[i][key] = float(doc[key])

        return {
            'times': configuration.times,
            'parameters': {
                'zone_code': zone_code,
                'date_from': date_from,
                'duration': duration
            },
            'len_of_data': len(data),
            'data': data
        }

class ReferenceZones(Resource):

    @configuration.measure_time
    def get(self):

        # Initialize global times keeper
        configuration.times = {}

        # Get query parameters
        time_added = request.args.get('time_added', default=None, type=str)
        country_fk = request.args.get('country_fk', default=None, type=str)
        ref_zone_id = request.args.get('ref_zone_id', default=None, type=str)

        # Get data
        data = dynamodb.get_ref_zones(time_added, country_fk, ref_zone_id)

        # Make them JSON serializable
        for i in range(len(data)):
            doc = data[i]
            for key in doc.keys():
                if isinstance(doc[key], Decimal):
                    data[i][key] = float(doc[key])

        return {
            'times': configuration.times,
            'len_of_data': len(data),
            'data': data
        }

    @configuration.measure_time
    def post(self):

        # Initialize global times keeper
        configuration.times = {}

        # Get query parameters
        ref_zone_id = request.args.get('ref_zone_id', default=None, type=str)

        # Generate ref_zone_id
        if ref_zone_id is None:
            ref_zone_id = randrange(410, 1000)
        else:
            ref_zone_id = int(ref_zone_id)

        # Check ID
        table = configuration.db.Table('ReferenceZones')
        get_item_response = table.get_item(Key={"Id": ref_zone_id})

        if 'Item' in get_item_response:
            abort(400, f"Reference Zone with ID {ref_zone_id} already exists.", statusCode=400)

        # Write dummy document
        else:
            new_doc = {
                "AreaRefAbbrev": "GREEK TESTING",
                "Country_FK": randrange(50, 100),
                "Id": int(ref_zone_id),
                "eicFunctionName_FK": None,
                "AreaTypeCode_FK": randrange(0, 100),
                "AreaRefAddedOn": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "MapCode_FK": randrange(0, 100),
                "AreaRefName": "[NEW] /GREEK",
                "AreaCode_eic_FK": None
            }

            table = configuration.db.Table('ReferenceZones')
            put_item_response = table.put_item(Item=new_doc)

            return {
                "times": configuration.times,
                "ref_zone_id": ref_zone_id,
                "put_item_response": put_item_response
            }

    @configuration.measure_time
    def delete(self):

        # Initialize global times keeper
        configuration.times = {}

        # Get query parameters
        ref_zone_id = request.args.get('ref_zone_id', default=None, type=str)

        # Generate ref_zone_id
        if not ref_zone_id:
            ref_zone_id = randrange(100, 1000)
        else:
            ref_zone_id = int(ref_zone_id)

        # Check ID
        table = configuration.db.Table('ReferenceZones')
        get_item_response = table.get_item(Key={"Id": ref_zone_id})

        if 'Item' not in get_item_response:
            abort(400, f"Reference Zone with ID {ref_zone_id} does not exist.", statusCode=400)
        else:
            doc = get_item_response['Item']
            if doc['AreaRefAbbrev'] != "GREEK TESTING":
                abort(400, f"Reference Zone with ID {ref_zone_id} cannot be deleted.", statusCode=400)

        # Delete document
        delete_item_response = table.delete_item(Key={"Id": ref_zone_id})

        return {
            "times": configuration.times,
            "ref_zone_id": ref_zone_id,
            "delete_item_response": delete_item_response
        }

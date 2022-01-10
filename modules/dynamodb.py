import configuration
import datetime
from boto3.dynamodb.conditions import Key

@configuration.measure_time
def query_ref_zones_zone_id(zone_id: int):
    table = configuration.db.Table('ReferenceZones')

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        KeyConditionExpression=Key('Id').eq(zone_id)
    )
    return response['Items']

@configuration.measure_time
def query_ref_zones_country_fk(country_fk: int):
    table = configuration.db.Table('ReferenceZones')

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        IndexName='Country_FK-AreaRefAddedOn-index',
        KeyConditionExpression=Key('Country_FK').eq(country_fk)
    )
    return response['Items']

@configuration.measure_time
def query_ref_zones_country_fk_date_range(country_fk: int, date_start: str):
    table = configuration.db.Table('ReferenceZones')

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        IndexName='Country_FK-AreaRefAddedOn-index',
        KeyConditionExpression=Key('Country_FK').eq(country_fk) & Key('AreaRefAddedOn').gte(date_start)
    )
    return response['Items']

@configuration.measure_time
def get_res_code(code_id: int):
    table = configuration.db.Table('ResolutionCodes')
    get_item_response = table.get_item(Key={"Id": code_id})

    return get_item_response['Item']

@configuration.measure_time
def query_energy_data(zone_code: int, date_range: (str, str)):
    table = configuration.db.Table('TotalLoadData')

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        KeyConditionExpression=Key('entsoeAreaReference_FK').eq(zone_code) & Key('DateTime').between(date_range[0], date_range[1])
    )

    return response['Items']

@configuration.measure_time
def get_energy_data(zone_code: int, date_from: str, duration: int, join: bool, light: bool) -> [dict]:

    # Initialize dicts which will be used for join
    reference_zones = {}
    resolution_codes = {}

    # Refactor DateTimes from string to date object in order calculate the "date_to"
    date_from = datetime.datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S')
    date_to = date_from + datetime.timedelta(days=duration)

    # Refactor them again to strings because they are stored in DynamoDB as strings
    date_from_str = datetime.datetime.strftime(date_from, '%Y-%m-%d %H:%M:%S')
    date_to_str = datetime.datetime.strftime(date_to, '%Y-%m-%d %H:%M:%S')

    results = query_energy_data(zone_code, (date_from_str, date_to_str))

    # Join energy data with information about resolution codes and reference zones
    final = []
    for doc in results:

        if join:
            # Join Reference Zone (caching technique)
            zone_code = doc['entsoeAreaReference_FK']
            if zone_code not in reference_zones.keys():
                ref_zone_doc = query_ref_zones_zone_id(zone_code)[0]
                reference_zones[zone_code] = ref_zone_doc
            doc['ReferenceZoneInfo'] = reference_zones[zone_code]

            # Join Resolution Codes (caching technique)
            resolution_code = doc['ResolutionCode_FK']
            if resolution_code not in resolution_codes.keys():
                res_code_doc = get_res_code(resolution_code)
                resolution_codes[resolution_code] = res_code_doc
            doc['ResolutionCodeInfo'] = resolution_codes[resolution_code]

        if light:
            keys = ['TotalLoadValue',
                    'DateTime',
                    'ResolutionCode_FK',
                    'ResolutionCodeInfo',
                    'entsoeAreaReference_FK',
                    'ReferenceZoneInfo']
            doc = {k: v for k, v in doc.items() if k in keys}

        final.append(doc)

    return final

@configuration.measure_time
def get_ref_zones(time_added: str, country_fk: str = None, ref_zone_id: str = None) -> [dict]:

    country_fk = int(country_fk) if country_fk else None

    # Get ref_zone with specific ID
    if ref_zone_id:
        table = configuration.db.Table('ReferenceZones')
        get_item_response = table.get_item(Key={"Id": int(ref_zone_id)})
        return [get_item_response['Item']]

    # Query ref zones which they are added later than the  "time_added"
    elif time_added:
        results = query_ref_zones_country_fk_date_range(int(country_fk), time_added)

    # Query ref zones with only Country_FK
    else:
        results = query_ref_zones_country_fk(int(country_fk))

    return results

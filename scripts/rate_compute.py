import pymysql
import os.path
from datetime import datetime
import json

from UserRate import UserRate

connection = pymysql.connect(host='sensoria-2.ics.uci.edu', port=3306, user='tippersUser', passwd='tippers2018', db='mysql')
database = "tippersdb_logs"
table = "API_LOG"

""" Get the email ids from tippersdb_restored DB and create a dictionary with
email as key and UserRate as the value
"""
def get_distinct_users():
    ur_list = {}
    d_users_sql = "SELECT distinct(email) from tippersdb_restored.USER"
    try: 
        with connection.cursor() as cursor:
            cursor.execute(d_users_sql)
            for row  in cursor:
                ur_list[row[0]] = UserRate(row[0])
    except Exception as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        return ur_list

def parse_payload(jsonstr):
    payload = json.loads(jsonstr)
    if 'subject_id' in payload:
        return payload['subject_id']
    else
        return ""

def get_rows():
    
    user_dict = get_distinct_users()

    total_users = len(user_dict)

    apis_to_look_for = ["/analytics/occupancy/area/get", "/analytics/occupancy/rooms/get", 
    "/observation/get", "/semanticobservation/getLast", "/semanticobservation/get/usersWOPolicy"]
    
    apis_query = "SELECT timeStamp, payload, api from tippersdb_logs.API_LOG where api IN ["

    for api in apis_to_look_for:
        apis_query+=api
        apis_query+= ","
    apis_query = apis_query[:-1]
    apis_query+= "]"

    print(apis_query)

    try: 
        with connection.cursor() as cursor:
            cursor.execute(apis_query)
            for row  in cursor:
                user_email = parse_payload(row['payload'])
                api = row['api']
                if not user_email: #subject_id not present
                    if api == "/analytics/occupancy/area/get" 
                        or api == "/analytics/occupancy/rooms/get": #occupancy api

                    else:


                else:

    except Exception as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        return distinct_users

def get_sample_data(from_ts, to_ts):
    if os.path.exists(sample_file): return
    sample_data = []
    sample_data_sql = 'SELECT userId, location, startTimestamp, endTimestamp from {}.{} ' \
                            'where startTimestamp >= "{}" and endTimestamp <= "{}"' \
                            .format(database, table, from_ts, to_ts)
    try: 
        with connection.cursor() as cursor:
            cursor.execute(sample_data_sql)
            writeToFile(sample_file, cursor)
    except Exception as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))


if __name__== "__main__":
    main()
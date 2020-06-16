import pymysql
import os.path
from datetime import datetime
import json
import random
import scipy.stats as stats

from UserRate import UserRate

connection = pymysql.connect(host='', port=3306, user='', passwd='', db='')

def get_distinct_users():
    ur_list = {}
    d_users_sql = "SELECT distinct(email) from USER"
    try: 
        with connection.cursor() as cursor:
            cursor.execute(d_users_sql)
            for row  in cursor:
                ur_list[row[0]] = UserRate(row[0])
        return ur_list
    except Exception as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
        return ur_list

def parse_payload(jsonstr):
    payload = json.loads(jsonstr)
    if 'subject_id' in payload:
        return payload['subject_id']
    else:
        return ""

def get_random_no():
    a, b = 1, 20
    mu, sigma = 6, 4
    dist = stats.truncnorm((a - mu) / sigma, (b - mu) / sigma, loc=mu, scale=sigma)
    values = dist.rvs(1)
    return int(values[0])

def get_rows(user_dict, apis_query):
    
    try: 
        with connection.cursor() as cursor:
            cursor.execute(apis_query)
            for row in cursor:
                user_email = parse_payload(row[0])
                subjects = []
                if not user_email: #subject_id not present
                    num_users = get_random_no()
                    for i in range(0, num_users):
                        subjects.append(user_dict[random.choice(list(user_dict.keys()))])
                else:
                    if user_email in user_dict:
                        subjects.append(user_dict[user_email])

                for sub in subjects:
                    sub.queries += 1/len(subjects)
    except Exception as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))


def main():
    user_dict = get_distinct_users()
    apis_to_look_for = ["/analytics/occupancy/area/get", "/analytics/occupancy/rooms/get", "/observation/get", "/semanticobservation/getLast", "/semanticobservation/get/usersWOPolicy"]
    
    apis_query = "SELECT payload from API_LOG where api IN ("

    for api in apis_to_look_for:
        apis_query+="\'" + api + "\'"
        apis_query+= ","
    apis_query = apis_query[:-1]
    apis_query+= ")"

    get_rows(user_dict, apis_query)

    for k, v in user_dict.items():
        print (k, v.queries)

if __name__== "__main__":
    main()

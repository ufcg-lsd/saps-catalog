#!/usr/bin/python3

import psycopg2
import json
import argparse

def parseArguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("catalog_address", help="Catalog Address", type=str)
    parser.add_argument("catalog_port", help="Catalog Port", type=str)
    parser.add_argument("catalog_db_name", help="Catalog Database Name", type=str)
    parser.add_argument("catalog_user_name", help="Catalog user",type=str)
    parser.add_argument("catalog_user_password", help="Catalog Password",type=str)

    #Optional Argument
    parser.add_argument("-n", "--number", help="Number of tasks to show", type=int, default=10)

    args = parser.parse_args()
    return args

def result_to_dic(query_result):
    result = str(query_result)[2:-3].replace("\'", "\"")

    tasks = json.loads(result)
    return tasks


def getTasks(mydb,total = 10):

    mycursor = mydb.cursor()

    query = "select json_agg(t) from (select * from tasks order by updated_time desc FETCH first " + str(total) + " rows only) t"

    # Get list of tasks sorted by updateTime
    mycursor.execute(query)

    query_result = mycursor.fetchall()

    tasks = result_to_dic(query_result)


    print("taskId | dataset | region | imageDate | state")

    for i in range(total):
        task = tasks[i]
        print(task['task_id'] + " | " + task['dataset'] + " | " + task['region'] + " | " + task['image_date'] + " | " + task['state'])

if __name__ == '__main__':
    # Parse the arguments
    args = parseArguments()

    mydb = psycopg2.connect(
    host=args.catalog_address,
    user=args.catalog_user_name,
    password=args.catalog_user_password,
    database=args.catalog_db_name,
    port=args.catalog_port
    )

    getTasks(mydb,args.number)

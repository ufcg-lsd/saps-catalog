#!/usr/bin/python3

import requests
import json
import operator
import argparse

def parseArguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("dispatcher_address", help="Dispatcher Address and Port", type=str)
    parser.add_argument("userEmail", help="User Email",type=str)
    parser.add_argument("userPass", help="User Password",type=str)

    #Optional Argument
    parser.add_argument("-n", "--number", help="Number of tasks to show", type=int, default=10)

    args = parser.parse_args()
    return args

def getTasks(url,headers,total = 10):

    # Send get request
    res = requests.get(url,headers = headers)

    # Get list of tasks and sort by updateTime
    tasks = json.loads(res.text)
    sorted_tasks = sorted(tasks,key=operator.itemgetter('updateTime'), reverse=True)

    # Check if the number of tasks to see is bigger than the number of tasks that exists
    if(total > len(sorted_tasks)):
        total = len(sorted_tasks)

    print("taskId | dataset | region | imageDate | state")

    for i in range(total):
        task = sorted_tasks[i]
        print(task['taskId'] + " | " + task['dataset'] + " | " + task['region'] + " | " + task['imageDate'] + " | " + task['state'])

if __name__ == '__main__':
    # Parse the arguments
    args = parseArguments()

    url = "http://" + args.dispatcher_address + "/processings"
    headers = {'userEmail': args.userEmail,'userPass': args.userPass}

    getTasks(url,headers,args.number)

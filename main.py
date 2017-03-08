import operator
import sys
from datetime import datetime, timedelta
import time

class Jobs(object):

    def __init__(self):
        self.q = []

    def add_task(self, timestamp, task_name, priority=0):
        temp_list=[]
        while len(self.q)!=0 and self.q[-1][0] < timestamp:
            temp_list.append(self.q.pop(-1))
        self.q.append([timestamp, task_name, priority])
        for el in temp_list:
            self.q.append(el)

    def get_task(self, timestamp):
        temp=[]
        while len(self.q)!=0 and self.q[-1][0] <= timestamp:
            temp.append(self.q.pop(-1))
        temp = sorted(temp, key = operator.itemgetter(0, 2))
        return temp


if __name__=="__main__":
    file_name, curr_timestamp = sys.argv[1:]
    curr_timestamp = datetime.strptime(curr_timestamp, '%Y/%m/%d %H:%M:%S')
    jobs = Jobs()

    print "Reading jobs from file"
    with open(file_name, 'r') as f:
        for line in f.readlines():
            timestamp, task_name, priority = line.split(",")
            timestamp = datetime.strptime(timestamp, '%Y/%m/%d %H:%M:%S')
            jobs.add_task(timestamp, task_name, int(priority))

    print "Starting Queue"
    while True:
        curr_timestamp = curr_timestamp + timedelta(minutes=1)
        for task in jobs.get_task(curr_timestamp):
            print "Processed job %s at time [%s]"%(task[1], curr_timestamp.strftime('%Y/%m/%d %H:%M:%S'))
        time.sleep(59)
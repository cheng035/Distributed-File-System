import csv
import datetime
import random
import string

# D1: Unique_Person_Name, location, start_time, end_time
# D2: Unique_Person_Name

def name_generator():
    first_name = ''.join(random.choice(string.ascii_lowercase) for i in range(6))
    last_name = ''.join(random.choice(string.ascii_lowercase) for i in range(5))
    name = first_name + " " + last_name
    return name

def time_generator():
    start_date = datetime.date(2020, 11, 22)
    end_date = datetime.date(2020, 11, 25)
    time_diff = end_date - start_date
    days_diff = time_diff.days
    random_days = random.randrange(days_diff)
    random_date = start_date + datetime.timedelta(days=random_days)
    random_date = random_date.strftime("%Y-%m-%d")

    rtime = int(random.random() * 86400)

    hours = int(rtime / 3600)
    minutes = int((rtime - hours * 3600) / 60)

    start_time = random_date + " " + '%02d:%02d:%02d' % (hours, minutes, rtime - hours * 3600 - minutes * 60)
    end_time = random_date + " " + '%02d:%02d:%02d' % (random.randrange(hours, 24), random.randrange(minutes, 60), rtime - hours * 3600 - minutes * 60)
    return start_time, end_time

locations = ["iSchool", "ECE Building", "Main Squad", "Illini Union", "Grainger", "UGL", "iHotel", "ARC", "Ice Arena"]
lines = []
names = []

for i in range(100000):
    num = random.randrange(0, 9)
    name = name_generator()
    lis = []
    lis.append(name)
    names.append(lis)

    curr_time = "2020-11-22 00:00:00"

    for j in range(num):
        start_time, end_time = time_generator()

        if start_time < curr_time: continue

        curr_time = end_time

        line = [name, locations[random.randrange(0, 9)], start_time, end_time]
        lines.append(line)

with open('org.csv', 'w',newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(lines)

with open('inf.csv', 'w',newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(names[0:10000])
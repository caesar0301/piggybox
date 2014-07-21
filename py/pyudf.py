#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "chenxm"
__email__ = "chenxm35@gmail.com"
import os
import sys
import logging
import re
import time
import math

def outputSchema(schema_str):
    def wrap(f):
        def wrapped_f(*args):
            return f(*args)
        return wrapped_f
    return wrap

@outputSchema("ts:bag{tv:(:double)}")
def diff(input):
    output = []
    last = None
    for t in input:
        num = float(t[0])
        if last is not None:
            output.append(tuple([num-last]))
        last = num
    return output

@outputSchema("record: {(rank:int, name:chararray, gpa:double,\
zipcode:chararray)}")
def enumerate_bag(input):
    output = []
    for rank, item in enumerate(input):
        output.append(tuple([rank] + list(item)))
    return output

###############################################################################
## UDFs below are used in OMNILAB, 2014
@outputSchema("ts:tuple")
def parse_timestr_utc(timestr):
    """ Clean timestamps in HZ mobile data, e.g.,
    16-8月 -12 11.44.10.922 上午
    17-AUG-12 01.29.07.727 PM
    """
    if timestr is None:
        return None
    result = None
    retime = re.compile(r"(\d{1,2})-([^-]*)-(\d{2,4})\s((?:\d+\.){3}\d+\s+[^\s]*)", re.IGNORECASE)
    def _hms_24(tstr):
        ## Extract hour.minute.seconds in 24-hour format
        tstr = tstr.strip("\r\t\n ")
        time, apm = re.split("\s+", tstr, 1)
        hour, minute = time.split(".", 1)
        hour = int(hour)
        apm = 0 if apm in ["AM", "上午"] else 1;
        if apm == 0:
            if hour >= 0 and hour <= 12:
                hour = hour % 12
        else:
            if hour >= 1 and hour <= 12:
                hour = 12 + hour % 12
        return "%02d.%s" % (hour, minute)
    ## Parse string
    match = retime.search(timestr)
    if match:
        day = int(match.group(1))
        rawmonth = match.group(2)
        month = rawmonth
        month_match = re.match(r'\d+', rawmonth)
        if month_match:
            month = int(month_match.group(0))
        elif month == 'AUG':
            month = 8
        elif month == 'OCT':
            month = 10
        year = int('20'+match.group(3))
        # parse time
        time24 = _hms_24(match.group(4))
        result = '%4d-%02d-%02dT%s' % (year, month, day, time24)
    return result

@outputSchema("ts:tuple")
def parse_timestr_epoch(timestr):
    """ Parse time string as epoch time, a.k.a. Unix Time
    """
    timestr = parse_timestr_utc(timestr)
    if timestr is None:
        return None
    sec, mlsec = timestr.rsplit(".", 1)
    unix_time = time.mktime(time.strptime(sec, "%Y-%m-%dT%H.%M.%S")) + \
        int(mlsec) * math.pow(10, -len(mlsec))
    return "%.3f" % unix_time


## Module test
if __name__ == '__main__':
    testtime = ['16-8月 -12 10.05.02.525000 上午', '7-AUG-12 11.05.07.418 PM']
    for t in testtime:
        print parse_timestr_utc(t)
        print parse_timestr_epoch(t)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "chenxm"
__email__ = "chenxm35@gmail.com"
import os, sys
import logging

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


## UDFs below are used in OMNILAB, 2014
@outputSchema("ts:tuple")
def parse_timestr_utc(timestr):
    if timestr is None:
        return None
    result = None
    retime = re.compile(r'(\d{1,2})-([^-]*)-(\d{2,4})\s((?:\d+\.){3}\d+)',
                        re.IGNORECASE)
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
        time = match.group(4)
        parts = time.split('.')
        result = '%4d-%02d-%02dT%s' % (year, month, day, time)
    return result


if __name__ == '__main__':
    testtime = ['16-8月 -12 10.00.00.201 上午', '17-AUG-12 10.05.07.418000 AM']
    for t in testtime:
        print parse_timestr_utc(t)

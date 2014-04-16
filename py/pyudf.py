import os, sys
import logging

def outputSchema(schema_str):
    def wrap(f):
        def wrapped_f(*args):
            return f(*args)
        return wrapped_f
    return wrap


########### Uesr defined functions #################
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

@outputSchema("record: {(rank:int, name:chararray, gpa:double, zipcode:chararray)}")
def enumerate_bag(input):
    output = []
    for rank, item in enumerate(input):
        output.append(tuple([rank] + list(item)))
    return output
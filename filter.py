# -*- coding: utf-8 -*-
'''
Created on 6 мар. 2017 г.
Производит фильтрацию неправильно оформленных записей
Запуск:
spark-submit filter.py <input file(s)> <output directory>
@author: igor
'''
from pyspark import SparkConf, SparkContext
import csv
from StringIO import StringIO
from datetime import datetime
import argparse

parser = argparse.ArgumentParser(description='Filtering invalid entries')
parser.add_argument('input', nargs=1, help='Input file(s) in hdfs')
parser.add_argument('output', nargs=1, help='Output directory in hdfs')
args = parser.parse_args()

def create_validate(permitted_types = set(['pay', 'rec'])):
    def validate(line):
        try:
            inp =  StringIO(line)
            reader = csv.reader(inp, delimiter=',')
            fields = list(reader)
            if len(fields) != 1:
                return False
            if len(fields[0]) != 4:
                return False
            id = int(fields[0][0])  # @ReservedAssignment
            if id < 0:
                return False
            if fields[0][1] not in permitted_types:
                return False
            value = int(fields[0][2])  # @UnusedVariable
            business_date = datetime.strptime(fields[0][3], '%Y-%m-%d')  # @UnusedVariable
            return True
        except:
            return False
    return validate


def main():
    conf = SparkConf().setAppName('Filter')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(args.input[0])
    rdd.filter(create_validate()).saveAsTextFile(args.output[0])

if __name__ == '__main__':
    main()


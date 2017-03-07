# -*- coding: utf-8 -*-
'''
Created on 6 мар. 2017 г.
Производит фильтрацию неправильно оформленных записей
Запуск:
spark-submit --py-files common.py filter.py <input file(s)> <output directory>
@author: igor
'''
from pyspark import SparkConf, SparkContext
import argparse
from common import create_validate

parser = argparse.ArgumentParser(description='Filtering invalid entries')
parser.add_argument('input', nargs=1, help='Input file(s) in hdfs')
parser.add_argument('output', nargs=1, help='Output directory in hdfs')
args = parser.parse_args()

def main():
    conf = SparkConf().setAppName('Filter')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(args.input[0])
    rdd.filter(create_validate()).saveAsTextFile(args.output[0])

if __name__ == '__main__':
    main()


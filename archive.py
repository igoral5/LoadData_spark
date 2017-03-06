# -*- coding: utf-8 -*-
'''
Created on 6 мар. 2017 г.
Производит разделение записей по дате. Записи с одинаковыми датами сохраняются в каталоге, имя которого
совпадает с датой.
Запуск:
spark-submit --jars MultipleOutputByKey.jar archive.py <input path> <output path>
где, MultipleOutputByKey.jar - jar собранный на основе https://github.com/igoral5/MultiOutputByKey
@author: igor
'''
from pyspark import SparkConf, SparkContext
import argparse
from StringIO import StringIO
import csv

parser = argparse.ArgumentParser(description='Sorting record by business date')
parser.add_argument('input', nargs=1, help='Input file(s) in hdfs')
parser.add_argument('output', nargs=1, help='Output directory in hdfs')
args = parser.parse_args()

def split_key_value(line):
    inp = StringIO(line)
    reader = csv.reader(inp, delimiter=',')
    fields = list(reader)
    return (fields[0][3], line)

def main():
    conf = SparkConf().setAppName('Archive')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile(args.input[0])
    rdd.map(split_key_value).saveAsHadoopFile(args.output[0], 'com.example.MultipleTextOutputFormatByKey')

if __name__ == '__main__':
    main()
    



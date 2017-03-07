# -*- coding: utf-8 -*-
'''
Created on 7 мар. 2017 г.
Общая часть для filter.py и archive.py
@author: igor
'''
import unittest
import csv
from StringIO import StringIO
from datetime import datetime

def create_validate(permitted_types = set([u'pay', u'rec'])):
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
            if fields[0][1].lower() not in permitted_types:
                return False
            value = int(fields[0][2])  # @UnusedVariable
            business_date = datetime.strptime(fields[0][3], '%Y-%m-%d')  # @UnusedVariable
            return True
        except:
            return False
    return validate

def split_key_value(line):
    inp = StringIO(line)
    reader = csv.reader(inp, delimiter=',')
    fields = list(reader)
    return (fields[0][3], line)

class Test(unittest.TestCase):

    def test_validate_wrong_line_1(self):
        validate = create_validate()
        self.assertFalse(validate(u'kjbvfyueweyt90ifcbv\nihfvguihgui\ndhfkj'))
    
    def test_validate_wrong_line_2(self):
        validate = create_validate()
        self.assertFalse(validate(u'""",""",",",,,,'))
    
    def test_validate_wrong_number_fields(self):
        validate = create_validate()
        self.assertFalse(validate(u'jkhfkjshfk,hfjoihfui,hfduifui,ueifgui,hdfsghjs'))
    
    def test_validate_wrong_id_1(self):
        validate = create_validate()
        self.assertFalse(validate(u'1a,"pay", 35,"2017-01-01"'))
    
    def test_validate_wrong_id_2(self):
        validate = create_validate()
        self.assertFalse(validate(u'-1,"pay", 35,"2017-01-01"'))
    
    def test_validate_wrong_type(self):
        validate = create_validate()
        self.assertFalse(validate(u'1,"payment", 35,"2017-01-01"'))
    
    def test_validate_wrong_value(self):
        validate = create_validate()
        self.assertFalse(validate(u'1,"pay", 35.0,"2017-01-01"'))
    
    def test_validate_wrong_date_1(self):
        validate = create_validate()
        self.assertFalse(validate(u'1,"pay", 35,"2017-01-40"'))
    
    def test_validate_wrong_date_2(self):
        validate = create_validate()
        self.assertFalse(validate(u'1,"pay", 35,"2017-13-01"'))
    
    def test_validate_wrong_date_3(self):
        validate = create_validate()
        self.assertFalse(validate(u'1,"pay", 35,"2017-02-29"'))
    
    def test_validate_good_date_1(self):
        validate = create_validate()
        self.assertTrue(validate(u'1,"pay", 35,"2016-02-29"'))
    
    def test_split_key_value_1(self):
        self.assertEqual(split_key_value(u'1,"pay", 35,"2016-02-29"'), (u'2016-02-29', u'1,"pay", 35,"2016-02-29"'))
    
    def test_split_key_value_2(self):
        with self.assertRaises(IndexError):
            split_key_value(u'')
    
    def test_split_key_value_3(self):
        with self.assertRaises(IndexError):
            split_key_value(u'fdvg"""sdffa,fa')
    
    def test_split_key_value_4(self):
        with self.assertRaises(IndexError):
            split_key_value(u'"ab","cd","ef"')
    
    def test_split_key_value_5(self):
        self.assertEqual(split_key_value(u'"ab","cd","ef","gh"'), (u'gh', u'"ab","cd","ef","gh"'))
    
if __name__ == "__main__":
    unittest.main()
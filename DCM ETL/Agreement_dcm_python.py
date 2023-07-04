#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Troy Zhongyi
# pipe delimited text file to CSV file
import csv

with open("L:\temporary\bhargavi\DCM_EXTRACTOR_CAPS_PAYMENT_RECORD.txt", "rt") as file_pipe:
    reader_pipe = csv.reader(file_pipe, delimiter='|')
    # Input directories here
    with open("DCM_EXTRACTOR_CAPS_PAYMENT_RECORD.csv", 'wt') as file_comma:
        writer_comma = csv.writer(file_comma, delimiter=',')
        writer_comma.writerows(reader_pipe)


# In[ ]:


# Fetch few columns from agreement Table of the dcm schema. Then store them into .txt file
import csv
import datetime
import traceback
import math
from calendar import monthrange
from datetime import timedelta

from os.path import join

from commons.dbutils.postgres import DataExporter
from commons.textfiles.delimited import DelimitedReader
from commons.sorting import SortedReader
from loader_product import ProductLoader
from loader_application_product import ProductLoaderApp
from dateutil.relativedelta import relativedelta
from _decimal import Decimal

DCM_SQL = '''SELECT
column1,
column2,
column3,
column4,
column5

FROM
dcm.agreement

WHERE
aaa = bbb
AND
ccc = ddd
AND
eee = fff'''

class Extractor():
    def __init__(self, resource_manager, config, conf_file_name, trace_logger, cycle_date, environment):
        db_alias = config['Resource-Links']['db']
        self.postgres_conn = resource_manager.get_resource(db_alias)
        self.cursor = self.postgres_conn.cursor
        
        self.conf_file_name = conf_file_name
        self.trace_logger = trace_logger
        
        sorting_writer_alias = config['Settings']['sorting_writer_alias']
        self.sorting_writer_factory = resource_manager.get_resource_factory(sorting_writer_alias)
        
        self.cycle_date = cycle_date
        self.previous_cycle_date = config['file']['prev_cycle_date']
    
    def extract_data_into_file(self, conn, sql, file_name):
        file_name = open(join(self.temp_dir, file_name), 'wb')
        s_writer = self.sorting_writer_factory.create(dest=file_name, text_mode=False)
        exporter = DataExporter(conn, sql)
        exporter.create_meta()
        meta_header = exporter.get_formatted_meta()
        file_name.write(meta_header.encode('utf-8'))
        file_name.write(b'\n')
        exporter.export_using_copy_to(s_writer)
        
    def load_agreement_dcmdata_into_file(self):
        '''Generate file from by extracting data from dcm database'''
        self.trace_logger.info('Extract for writing agent Started')
        try:
            str_date = '-'.join([self.cycle_date[:4], self.cycle_date[4,6], self.cycle_date[6:]])
            prev_str_date = '-'.join([self.previous_cycle_date[:4], self.previous_cycle_date[4:6], self.previous_cycle_date[6:]])
            agreement_dcm_log = DCM_SQL.format('\'' + prev_str_date + '\'')
            self.extract_data_into_file(self.postgres_conn, agreement_dcm_log, 'agreement_dcm_log.txt')
        
        except Exception:
            self.trace_logger.fatal("Fatal Exception Detected", exc_info=1)
            traceback.print_exc()
            exit(1)
            
    def process_dcm_data(self):
        '''read data from json files and perform sort and merge'''
        agreement_reader = self.open_file_reader('agreement_dcm_log.txt')
        
    def generate_agreementdcm_extract(self):
        self.process_dcm_data()


# In[ ]:





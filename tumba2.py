#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri May 25 14:23:47 2018

@author: ian
"""

import os
import pandas as pd
import pdb
import xlrd

# standard initialisations    
file_path = '/home/ian/Desktop'
file_str = 'gasprof'
sheet_name = 'gas_data'
header_row = 9
heights = [0.45, 4.56, 10.24, 18.07, 26.28, 34.39, 42.58, 54.39, 70.14]

def func(f):
    book = xlrd.open_workbook(f)
    sheet = book.sheet_by_name(sheet_name)
    date_time = map(lambda x: xlrd.xldate_as_datetime(x, datemode = book.datemode), 
                    sheet.col_values(0, header_row + 1))
    col_names = sheet.row_values(9)
    df = pd.DataFrame(index = date_time)
    for var in [('CO2', 'CO2'), ('T', 'Tair'), ('P', 'ps'), ('MFM','Flow')]:
        old_vars_list = sorted(filter(lambda x: var[0] in x, col_names))
        new_vars_list = map(lambda x: '{0}_{1}m'.format(var[1], str(x)), heights)
        for i, this_var in enumerate(old_vars_list):
            idx = col_names.index(this_var)
            df[new_vars_list[i]] = pd.to_numeric(sheet.col_values(idx, 
                                                                  header_row + 1),
                                                 errors = 'coerce')
    return df.resample('1T').mean().interpolate().resample('60T').pad()

file_list = filter(lambda x: file_str in x, os.listdir(file_path))
file_list = map(lambda x: os.path.join(file_path, x), file_list)
df = pd.concat(map(lambda x: func(x), file_list))
df.sort_index(inplace = True)
df.drop_duplicates(inplace = True)
df = df[~df.index.duplicated(keep = 'first')]
new_date_index = pd.date_range(df.index[0], df.index[-1], freq='60T')
df = df.reindex(new_date_index)
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 13:32:09 2017

@author: ian
"""

import datetime as dt
import numpy as np
import os
import pandas as pd
import pdb
import xlrd

#------------------------------------------------------------------------------
def align_data(df_list):
    begin_list, end_list = [], []
    for df in df_list:
        begin_list.append(df.index[0])
        end_list.append(df.index[-1])
    begin_date = max(begin_list)
    end_date = min(end_list)
    new_index = pd.date_range(begin_date, end_date, freq = '30T')
    new_df = df_list[0].join(df_list[1])
    return new_df.reindex(new_index)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def drop_data(df, date_list):
    for l in date_list:
        if l[0] == '-':
            l = [dt.datetime.strftime(df.index[0].to_pydatetime(), 
                                      '%Y-%m-%d %H:%M:%S'), 
                 l[1]]
        elif l[1] == '-':
            l = [l[0],
                 dt.datetime.strftime(df.index[-1].to_pydatetime(), 
                                      '%Y-%m-%d %H:%M:%S')]
        df.loc[l[0]: l[1]] = np.nan
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def filter_data(df, var, limits):
    df.loc[df[var] < limits[0], var] = np.nan
    df.loc[df[var] > limits[1], var] = np.nan
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def get_irga_data(path):
    print 'Processing file: {}'.format(os.path.basename(path))
    df = pd.read_csv(path, skiprows = [0, 2, 3], na_values = 'NaN', 
                     error_bad_lines = False, dtype = {'CO2_Li820': 'float',
                                                       'Level_&_Sample': 'int'})
    df.index = pd.to_datetime(df.TIMESTAMP, errors = 'coerce')
    df = df[pd.notnull(df.index)]
    df.drop('TIMESTAMP', axis = 1, inplace = True)
    df['level'] = [str(x)[0] for x in df['Level_&_Sample']]
    prep_data(df)
    filter_data(df, 'CO2_Li820', [300, 900])
    return process_irga_data(df)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def get_file_list(path, word):
    f_list = filter(lambda x: word in x, os.listdir(path))
    return sorted(map(lambda x: os.path.join(path, x), f_list))
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def get_met_data(path):
    header_rows = [0, 1, 2]
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_name('Data')
    var_list = sheet.row_values(0)
    T_var_list = sorted(filter(lambda x: 'Ta_CS' in x, var_list))
    p_var_list = ['Ps_PTB110_Avg']
    date_time = map(lambda x: xlrd.xldate_as_datetime(x, 
                                                      datemode = book.datemode), 
                    sheet.col_values(0, header_rows[-1] + 1))
    df = pd.DataFrame(index = date_time)
    heights_dict = dict(zip(T_var_list, 
                            map(lambda x: 'Tair_{}m'.format(x), heights_list)))
    for var in T_var_list:
        idx = var_list.index(var)
        name = heights_dict[var]
        df[name] = sheet.col_values(idx, header_rows[-1] + 1)
    for var in p_var_list:
        idx = var_list.index(var)
        df['ps'] = sheet.col_values(idx, header_rows[-1] + 1)
    prep_data(df)
    return df    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_date_iterator(df):

    marker = df['Level_&_Sample'] - df['Level_&_Sample'].shift()
    date_list = marker[~marker.isin([1, 91])].index
    int_start_list = map(lambda x: df.index.get_loc(x), date_list)
    int_end_list = list(np.array(int_start_list[1:]) - 1) + [len(df) - 1]
    return zip(df.iloc[int_start_list].index, df.iloc[int_end_list].index)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def make_result_dataframe(df):
    index = pd.date_range(df.index[0].round('min'), 
                          df.index[-1].round('min'),
                          freq = 'T')
    columns = ['CO2_{}m'.format(x) for x in heights_list]
    return pd.DataFrame(index = index, columns = columns, dtype = 'float')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def prep_data(df):
    df.sort_index(inplace = True)
    df.drop_duplicates(inplace = True)
    df = df[~df.index.duplicated(keep = 'first')]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def process_irga_data(df):
    print 'Parsing dates: '
    result_df = make_result_dataframe(df)
    levels_ref_dict = dict(zip([str(x) for x in range(1, 7)],
                               ['CO2_{}m'.format(x) for x in heights_list]))
    for date_pair in make_date_iterator(df):
        sub_df = df.loc[date_pair[0]: date_pair[1]].copy()
        index_name = ((sub_df.index[0] + (sub_df.index[-1] - 
                       sub_df.index[0]) / 2)
                      .round('min'))
        mean_df = sub_df.groupby('level').mean()['CO2_Li820']
        for level in mean_df.index:
            col_name = levels_ref_dict[level]
            result_df.loc[index_name, col_name] = mean_df[level]
        print index_name
    return result_df
#------------------------------------------------------------------------------ 

# Set some constants
path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/RobsonCreek'
#path = '/home/ian/Desktop/Robson'
heights_list = [1, 2, 3.5, 9, 21, 39]
bad_data_list = [['2017-05-10 00:00:00', '-']]

# Construct, process, smooth and downsample IRGA dataset
irga_fp_list = get_file_list(path, 'fast_profile')
irga_df = pd.concat(map(lambda x: get_irga_data(x), irga_fp_list))
prep_data(irga_df)
irga_df = irga_df.resample('2T').mean()
irga_df = irga_df.resample('30T').pad()

# Get met dataset
met_fp_list = get_file_list(path, 'RBS')
met_df = pd.concat(map(lambda x: get_met_data(x), met_fp_list))
prep_data(met_df)

# Join the datasets
df = align_data([irga_df, met_df])
drop_data(df, bad_data_list)




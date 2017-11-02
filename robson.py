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

#------------------------------------------------------------------------------    
def filter_data(df, var, limits):
    df.loc[df[var] < limits[0], var] = np.nan
    df.loc[df[var] > limits[1], var] = np.nan
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def get_data(f_path):
    print 'Processing file: {}'.format(os.path.basename(f_path))
    try:
        df = pd.read_csv(f_path, skiprows = [0, 2, 3], na_values = 'NaN', 
                         error_bad_lines = False, dtype = {'CO2_Li820': 'float',
                                                           'Level_&_Sample':
                                                               'int'})
    except Exception, e:
        pdb.set_trace()
    df.index = pd.to_datetime(df.TIMESTAMP, errors = 'coerce')
    df = df[pd.notnull(df.index)]
    df.drop('TIMESTAMP', axis = 1, inplace = True)
    prep_data(df)
    filter_data(df, 'CO2_Li820', [300, 900])
    return df
    return process_data(df)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def get_file_list(path, word):
    f_list = filter(lambda x: word in x, os.listdir(path))
    return sorted(map(lambda x: os.path.join(path, x), f_list))
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def make_date_iterator(df):

    def iterate_level_and_sample(id_num):
        level = int(num[0])
        sample = int(num[1:])
        if sample < 10:
            sample = sample + 1
        else:
            sample = 1
            level = level + 1
        return str(level) + str(sample).zfill(2)

    ref_num = '000'
    start_list = []
    for i, case in enumerate(df.index):
        num = str(df.loc[case, 'Level_&_Sample'])
        if not num == ref_num:
            start_list.append(i)
        ref_num = iterate_level_and_sample(num)
    end_list = list(np.array(start_list[1:]) - 1) + [len(df) - 1]
    return zip([df.iloc[x].name for x in start_list],
               [df.iloc[x].name for x in end_list])
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
def process_data(df):
    print 'Parsing dates: '
    result_df = make_result_dataframe(df)
    for date_pair in make_date_iterator(df):
        sub_df = df.loc[date_pair[0]: date_pair[1]].copy()
        index_name = ((sub_df.index[0] + (sub_df.index[-1] - 
                       sub_df.index[0]) / 2)
                      .round('min'))
        sub_df['level'] = [str(x)[0] for x in sub_df['Level_&_Sample']]
        levels_ref_dict = dict(zip([str(x) for x in range(1, 7)],
                                   ['CO2_{}m'.format(x) for x in heights_list]))
        try:
            mean_df = sub_df.groupby('level').mean()['CO2_Li820']
        except:
            pdb.set_trace()
        for level in mean_df.index:
            col_name = levels_ref_dict[level]
            result_df.loc[index_name, col_name] = mean_df[level]
        print index_name
    return result_df
#------------------------------------------------------------------------------ 

# Set some constants
#path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/RobsonCreek/Robson_CR1k_fast_profile_2017-01.dat'
path = '/home/ian/Desktop/Robson'
heights_list = [1, 2, 3.5, 9, 21, 39]

# Construct, process, smooth and downsample IRGA dataset
irga_fp_list = get_file_list(path, 'fast_profile')
irga_fp_list = ['/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/RobsonCreek/Robson_CR1k_fast_profile_2016-01.dat']
irga_df = pd.concat(map(lambda x: get_data(x), irga_fp_list))
irga_df['date'] = irga_df.index.to_pydatetime()
irga_df['delta'] = map(lambda x: x.total_seconds(), 
                       irga_df.date-irga_df.date.shift())
locs = map(lambda x: irga_df.index.get_loc(x), 
           irga_df[irga_df.delta > 0.2].index)



#result_df = make_result_dataframe(irga_df, heights_list)
#date_iterator = make_date_iterator(irga_df)
#process_data(irga_df, date_iterator, heights_list)
#result_df = result_df.rolling(window = 2).mean()
#result_df = result_df.resample('30T').mean()

# Read met dataset
#irga_fp_list = get_file_list(path, 'fast_profile')
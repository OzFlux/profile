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
def make_result_dataframe(df, heights_list):
    index = pd.date_range(df.index[0].round('min'), 
                          df.index[-1].round('min'),
                          freq = 'T')
    columns = ['CO2_{}m'.format(x) for x in heights_list]
    return pd.DataFrame(index = index, columns = columns)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def process_data_segment(sub_df, heights_list):
    index_name = ((sub_df.index[0] + (sub_df.index[-1] - sub_df.index[0]) / 2)
                  .round('min'))
    sub_df['level'] = [str(x)[0] for x in sub_df['Level_&_Sample']]
    levels_ref_dict = dict(zip([str(x) for x in range(1, 7)],
                               ['CO2_{}m'.format(x) for x in heights_list]))
    mean_df = sub_df.groupby('level').mean()['CO2_Li820']
    for level in mean_df.index:
        col_name = levels_ref_dict[level]
        result_df.loc[index_name, col_name] = mean_df[level]
    print index_name
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def filter_data(df, var, limits):
        df.loc[df[var] < limits[0], var] = np.nan
        df.loc[df[var] > limits[1], var] = np.nan
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def get_file_list(path, word):
    f_list = filter(lambda x: word in x, os.listdir(path))
    return map(lambda x: os.path.join(path, x), f_list)
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------    
def open_data(f_path):
    df = pd.read_csv(f_path, skiprows = [0, 2, 3], na_values = 'NaN')
    df.index = pd.to_datetime(df.TIMESTAMP)
    return df
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def prep_data(df):
    df.sort_index(inplace = True)
    df.drop_duplicates(inplace = True)
    df = df[~df.index.duplicated(keep = 'first')]
#------------------------------------------------------------------------------

# Set some constants
#path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/RobsonCreek/Robson_CR1k_fast_profile_2017-01.dat'
path = '/home/ian/Desktop/Robson_CR1k_fast_profile_2017-02.dat'
heights_list = [2, 4, 8, 16, 32, 64]
levels_ref_dict = dict(zip([str(x) for x in range(1, len(heights_list) + 1)],
                           ['CO2_{}m'.format(str(x)) for x in heights_list]))

#irga_fp_list = get_file_list(path, 'fast_profile')
irga_fp_list = ['/home/ian/Desktop/Robson_CR1k_fast_profile_2017-02.dat']
irga_df = pd.concat(map(lambda x: open_data(x), irga_fp_list))
prep_data(irga_df)
filter_data(irga_df, 'CO2_Li820', [300, 900])
result_df = make_result_dataframe(irga_df, heights_list)
process_data(irga_df)

# Get date pairs that mark the beginning and end of each contiguous data segment
date_iterator = make_date_iterator(irga_df)

# Iterate over dates
for date_pair in date_iterator:
    process_data_segment(irga_df.loc[date_pair[0]: date_pair[1]].copy(),
                         heights_list)
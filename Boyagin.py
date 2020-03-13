#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 07:22:44 2020

@author: imchugh
"""

import numpy as np
import pandas as pd
import xarray as xr

def stack_df_to_series(this_df, height_list, name):

    this_df.columns = height_list
    stacked_series = this_df.stack()
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series

path = '/home/unimelb.edu.au/imchugh/Downloads/Boyagin_CO2_prof_IRGA_avg.dat'
vars_to_import = ['TIMESTAMP', 'Cc_LI840_0_5m',  'Cc_LI840_1m',  'Cc_LI840_3m',
                  'Cc_LI840_6m', 'Cc_LI840_10m', 'Cc_LI840_16m',
                  'Cc_LI840_23m', 'Cc_LI840_30m', 'T_panel_Avg', 'P_atm_Avg']
date_start = '2019-09-20 16:00'

df = pd.read_csv(path, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                 index_col=['TIMESTAMP'], usecols=vars_to_import,
                 dtype={x: 'float' for x in vars_to_import})
df = df.loc[date_start:]
df = df.drop_duplicates()
df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq='2T'))
df = df[np.mod(df.index.minute, 30) < 4]
df = df.resample('30T').mean()
CO2_list = [x for x in df.columns if 'Cc' in x]
heights = np.array([float('.'.join(x.split('_')[2:]).replace('m', ''))
                    for x in CO2_list])

# Stack the data
CO2_series = stack_df_to_series(df[CO2_list], heights, 'CO2')
Ta_series = stack_df_to_series(
    pd.DataFrame({x: df['T_panel_Avg'] for x in heights}), heights, 'Tair')
P_series = stack_df_to_series(
    pd.DataFrame({x: df['P_atm_Avg'] / 10 for x in heights}), heights, 'P')
ds = pd.concat([CO2_series, Ta_series, P_series], axis=1).to_xarray()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 07:22:44 2020

@author: imchugh
"""

import numpy as np
import pandas as pd

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_path, vars_to_import):

    return (
        pd.read_csv(path, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                    index_col=['TIMESTAMP'], usecols=vars_to_import,
                    dtype={x: 'float' for x in vars_to_import})
        .loc[date_start:]
        .drop_duplicates()
        .pipe(reindex_data)
        .pipe(resample_data)
           )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reindex_data(df):

    return df.reindex(pd.date_range(df.index[0], df.index[-1], freq='2T'))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def resample_data(df):

    return df[np.mod(df.index.minute, 30) < 4].resample('30T').mean()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def stack_to_series(df, name):

    heights = [float('.'.join(x.split('_')[2:]).replace('m', ''))
               for x in df.columns]
    df.columns = heights
    stacked_series = df.stack()
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(dir_path=None):

    # Use internally defined path unless explicitly passed other
    if not dir_path: dir_path = path

    # Open the data
    df = open_data(dir_path, vars_to_import)

    # Construct co2 df
    co2_series = stack_to_series(df[[x for x in df.columns if 'Cc' in x]], 'CO2')

    # Construct temperature df
    ta_series = (
        pd.concat([df.T_panel_Avg.copy() for i in range(6)], axis=1,
                  ignore_index=True)
        .rename({i: x.replace('Cc_LI840', 'T_air')
                  for i, x in enumerate(df.columns) if 'Cc' in x}, axis=1)
        .pipe(stack_to_series, 'Tair')
                )

    # Construct pressure df (convert to kPa from hPa - div by factor 10)
    p_series = (
        pd.concat([df.P_atm_Avg.copy() for i in range(6)], axis=1,
                  ignore_index=True)
        .rename({i: x.replace('Cc_LI840', 'P_atm')
                  for i, x in enumerate(df.columns) if 'Cc' in x}, axis=1)
        .pipe(stack_to_series, 'P')
               ) / 10

    return pd.concat([co2_series, ta_series, p_series], axis=1).to_xarray()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBALS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
date_start = '2019-09-20 16:00'
vars_to_import = ['TIMESTAMP', 'Cc_LI840_0_5m',  'Cc_LI840_1m',  'Cc_LI840_3m',
                  'Cc_LI840_6m', 'Cc_LI840_10m', 'Cc_LI840_16m',
                  'Cc_LI840_23m', 'Cc_LI840_30m', 'T_panel_Avg', 'P_atm_Avg']
path = '/home/unimelb.edu.au/imchugh/Downloads/Boyagin_CO2_prof_IRGA_avg.dat'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

if __name__ == "__main__":

    ds = get_data()
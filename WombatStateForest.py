#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:14:25 2020

@author: imchugh
"""

import numpy as np
import os
import pandas as pd

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def drop_data(df, drop_list):
    for date_pair in drop_list:
        df.loc[date_pair[0]: date_pair[1]] = np.nan
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def filter_data(df, limits):

    return df.where((df >= limits[0]) & (df <= limits[1]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_heights(columns):

    return [int(x.split('_')[-1].replace('m', '')) for x in columns]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_dir, word, vars_to_import, freq):

    file_list = list(filter(lambda x: word in x, os.listdir(file_dir)))
    if len(file_list) == 0: print('No files found containing searchphrase {}'
                                  .format(word))
    df_list = []
    for f in file_list:
        full_path = os.path.join(file_dir, f)
        try:
            df_list.append(
                pd.read_csv(full_path, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                            index_col=['TIMESTAMP'], usecols=vars_to_import,
                            na_values='NAN', error_bad_lines=False,
                            dtype={x: 'float' for x in vars_to_import}))
        except ValueError:
            continue
    return_df = pd.concat(df_list)
    return_df.sort_index(inplace = True)
    return_df = return_df[~return_df.index.duplicated(keep = 'first')]
    new_index = pd.date_range(return_df.index[0], return_df.index[-1], freq=freq)
    return return_df.reindex(new_index)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def resample_data(df):

    return df[np.mod(df.index.minute, 30) < 4].resample('30T').mean()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def stack_to_series(df, name):

    heights = [int(x.split('_')[-1].replace('m', '')) for x in df.columns]
    df.columns = heights
    stacked_series = df.stack(dropna=False)
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(path):

    """Main function for converting raw data to profile-ready xarray format"""

    # Construct CO2 series
    co2_series = (
        open_data(file_dir=path, word='IRGA',
                  vars_to_import=irga_vars_to_import,
                  freq='2T')
        .pipe(resample_data)
        .pipe(drop_data, irga_drop_list)
        .pipe(filter_data, [300, 600])
        .pipe(stack_to_series, 'CO2')
                 )

    # Construct temp and pressure df
    met_df = (
        open_data(file_dir=path, word='met', vars_to_import=met_vars_to_import,
                  freq='30T')
        .pipe(drop_data, met_drop_list)
        .reindex(co2_series.index.get_level_values(0).unique())
             )

    # Construct temperature series
    ta_series = (
        pd.concat([met_df.Ta_HMP_02_Avg.copy() for i in range(6)], axis=1,
                  ignore_index=True)
        .rename({i: 'Tair_{}m'.format(str(x))
                  for i, x in enumerate(co2_series.index.levels[1])}, axis=1)
        .pipe(stack_to_series, 'Tair')
                )

    # Construct pressure series
    ps_series = (
        pd.concat([met_df.ps_7500_Avg.copy() for i in range(6)], axis=1,
                  ignore_index=True)
        .rename({i: 'P_{}m'.format(str(x))
                  for i, x in enumerate(co2_series.index.levels[1])}, axis=1)
        .pipe(stack_to_series, 'P')
                )

    # Create xarray dataset
    return pd.concat([co2_series, ta_series, ps_series], axis=1).to_xarray()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBALS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
irga_drop_list = [['2011-11-09 12:00', '2012-03-04 15:00'],
                  ['2012-11-10 13:00:00', '2013-03-04 16:00:00'],
                  ['2014-07-16 12:00', '2014-12-10 09:00:00'],
                  ['2015-10-06 03:00:00', '2020-02-03 08:00:00']]
irga_vars_to_import=['TIMESTAMP', 'Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m',
                     'Cc_LI840_8m', 'Cc_LI840_15m', 'Cc_LI840_30m']
met_drop_list = [['2014-05-21 13:00:00', '2014-06-18 16:30:00']]
met_vars_to_import=['TIMESTAMP', 'Ta_HMP_02_Avg', 'ps_7500_Avg']
#------------------------------------------------------------------------------
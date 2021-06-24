#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:14:25 2020

@author: imchugh
"""

import numpy as np
import pandas as pd
import pdb
import profile_utils as pu

#-----------------------------------------------------------------------------
### Variables ###
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
irga_drop_list = [['2011-11-09 12:00', '2012-03-04 15:00'],
                  ['2012-11-10 13:00:00', '2013-03-04 16:00:00'],
                  ['2014-07-16 12:00', '2014-12-10 09:00:00'],
                  ['2015-10-06 03:00:00', '2019-11-05 11:54:00']]
irga_baseline_offset_dict = {'first_offset': 
                             {'start_date': '2019-11-05 12:00:00',
                              'end_date': '2020-01-29 10:00:00',
                              'offset': 300}}
irga_vars_to_import=['TIMESTAMP', 'Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m',
                     'Cc_LI840_8m', 'Cc_LI840_15m', 'Cc_LI840_30m']
met_drop_list = [['2014-05-21 13:00:00', '2014-06-18 16:30:00']]
met_vars_to_import=['TIMESTAMP', 'Ta_HMP_02_Avg', 'ps_7500_Avg']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### Functions ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def adjust_CO2_data(df, adjust_dict):

    add_df = pd.DataFrame(data=0, index=df.index, columns=df.columns)
    for offset in adjust_dict:
        inner_dict = adjust_dict[offset]
        add_df.loc[inner_dict['start_date']: inner_dict['end_date']] = (
            inner_dict['offset']
            )
    return df + add_df
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
def merge_Tair_data(df):
    
    pass
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
def get_data():

    """Main function for converting raw data to profile-ready xarray format"""

    # Construct CO2 series
    irga_path = pu.get_path(state='raw', series='profile', 
                            site='WombatStateForest')
    co2_series = (
        pu.open_data(file_dir=irga_path, search_str='IRGA_avg',
                     vars_to_import=irga_vars_to_import,
                     freq='2T')
        .pipe(resample_data)
        .pipe(drop_data, irga_drop_list)
        .pipe(adjust_CO2_data, irga_baseline_offset_dict)
        .pipe(filter_data, [300, 600])
        .pipe(stack_to_series, 'CO2')
                 )
    
    # Construct temp and pressure df
    met_path = pu.get_path(state='raw', series='flux_slow', 
                           site='WombatStateForest', check_exists=True)
    met_df = (
        pu.open_data(file_dir=met_path, search_str='met', 
                     vars_to_import=met_vars_to_import, freq='30T')
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
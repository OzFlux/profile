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
                              'end_date': '2020-07-14 00:00:00',
                              'offset': 300}}
irga_vars_to_import=['Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m',
                     'Cc_LI840_8m', 'Cc_LI840_15m', 'Cc_LI840_30m']
met_drop_dict = {'Ta_HMP_01_Avg': [['2012-11-10 12:00:00', 
                                    '2012-11-23 17:00:00'],
                                   ['2014-05-21 13:00:00', 
                                    '2014-06-18 16:30:00'],
                                   ['2017-03-22 00:00:00',
                                    '2017-03-24 00:00:00'],
                                   ['2017-04-01 00:07:00',
                                    '2017-04-01 00:09:00'],
                                   ['2017-04-08 15:00:00',
                                    '2017-04-13 00:00:00'],
                                   ['2017-04-21 00:00:00',
                                    '2017-04-23 00:00:00'],
                                   ['2017-04-24 05:00:00',
                                    '2017-04-24 07:00:00'],
                                   ['2017-04-25 06:00:00',
                                    '2017-04-26 12:00:00'],
                                   ['2017-04-27 15:00:00',
                                    '2017-04-27 15:00:00'],
                                   ['2017-04-27 21:00:00',
                                    '2017-04-29 12:00:00'],
                                   ['2017-04-30 00:00:00',
                                    '2017-04-30 09:00:00'],
                                   ['2017-05-01 18:00:00',
                                    '2017-05-02 21:00:00'],
                                   ['2017-05-07 07:30:00',
                                    '2017-05-07 09:00:00'],
                                   ['2017-05-07 19:00:00',
                                    '2017-05-09 19:00:00'],
                                   ['2017-05-18 22:30:00',
                                    '2017-05-19 01:00:00'],
                                   ['2017-05-19 19:30:00',
                                    '2017-05-19 19:30:00'],
                                   ['2017-05-20 09:00:00',
                                    '2017-05-22 06:00:00'],
                                   ['2017-05-22 11:00:00',
                                    '2017-05-22 15:00:00'],
                                   ['2017-05-23 08:00:00',
                                    '2017-05-23 08:00:00'],
                                   ['2017-05-24 00:00:00',
                                    '2017-05-31 00:00:00'],
                                   ['2017-06-05 12:00:00',
                                    '2017-06-09 18:00:00'],
                                   ['2017-06-10 02:00:00',
                                    '2017-06-10 04:00:00'],
                                   ['2017-06-13 09:00:00',
                                    '2017-06-14 15:00:00'],
                                   ['2017-06-18 04:00:00',
                                    '2017-06-18 04:30:00']],
                 'Ta_HMP_02_Avg': [['2014-05-01 12:00:00', 
                                    '2014-06-12 14:00:00'],
                                   ['2019-10-12 09:00:00', 
                                    '2019-10-15 00:00:00'],
                                   ['2019-10-15 11:30:00', 
                                    '2019-10-15 15:30:00'],
                                   ['2020-01-19 10:30:00', 
                                    '2020-01-19 11:00:00'],
                                   ['2020-01-20 11:00:00', 
                                    '2020-01-20 14:00:00'],
                                   ['2020-01-21 09:00:00', 
                                    '2020-01-21 15:00:00'],
                                   ['2020-01-22 10:00:00', 
                                    '2020-01-22 14:00:00'],
                                   ['2020-01-25 20:00:00', 
                                    '2020-01-25 20:30:00'],
                                   ['2020-01-26 09:00:00', 
                                    '2020-01-26 20:00:00'],
                                   ['2020-01-27 18:30:00', 
                                    '2020-01-21 19:00:00'],
                                   ['2020-01-28 17:00:00', 
                                    '2020-01-28 21:00:00'],
                                   ['2020-02-10 21:00:00', 
                                    '2020-02-11 21:00:00'],
                                   ['2020-02-12 15:00:00', 
                                    '2020-02-12 22:30:00'],
                                   ['2020-02-16 11:00:00', 
                                    '2020-02-17 11:00:00'],
                                   ['2020-02-18 00:00:00', 
                                    '2020-02-21 00:00:00'],
                                   ['2020-02-22 11:00:00', 
                                    '2020-02-23 12:00:00'],
                                   ['2020-02-24 00:00:00', 
                                    '2020-02-25 12:00:00'],
                                   ['2020-02-27 00:00:00', 
                                    '2020-04-11 00:00:00']]}
met_vars_to_import = ['Ta_HMP_01_Avg', 'Ta_HMP_02_Avg', 'ps_7500_Avg', 
                      'Ta_HMP_Upr_Avg', 'Ta_HMP_Lwr_Avg', 'Tpanel_Avg']
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
def drop_data(df, drop_dates, drop_vars=None):

    if not drop_vars: drop_vars = df.columns
    for date_pair in drop_dates:
        df.loc[date_pair[0]: date_pair[1], drop_vars] = np.nan
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def filter_data(df, limits, drop_vars=None):
    
    if not drop_vars:
        return df.where((df >= limits[0]) & (df <= limits[1]))
    drop_df = df[drop_vars]
    no_drop_vars = [x for x in df.columns if not x in drop_vars]
    drop_df = drop_df.where((drop_df >= limits[0]) & (drop_df <= limits[1]))
    return pd.concat([df[no_drop_vars], drop_df], axis=1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_heights(columns):

    return [int(x.split('_')[-1].replace('m', '')) for x in columns]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def interpolate_T(df, T_height):

    upper_height, lower_height = 36, 2
    dtdz = (df.Ta_HMP_01_Avg - df.Ta_HMP_02_Avg) / -(upper_height - lower_height)
    return df.Ta_HMP_02_Avg + (T_height - upper_height) * dtdz
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def merge_Tair_data(df):
    
    met_alias_dict = {'Ta_HMP_02_Avg': 'Ta_HMP_Lwr_Avg', 
                      'Ta_HMP_01_Avg': 'Ta_HMP_Upr_Avg'}
    for key in met_alias_dict.keys():
        old_name, new_name = met_alias_dict[key], key
        the_series = pd.concat([df[old_name].dropna(), 
                                df[new_name].dropna()])
        the_series = the_series.reindex(df.index)
        df.drop([old_name, new_name], axis=1, inplace=True)
        df[new_name] = the_series
        df = drop_data(df, met_drop_dict[key], drop_vars=[key])
    return df
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

    # # Construct CO2 series
    irga_path = pu.get_path(site='WombatStateForest', series='Profile', 
                            state='Raw', check_exists=True)
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
    met_path = pu.get_path(site='WombatStateForest', series='Meteorology', 
                           state='Raw', check_exists=True)
    met_df = (
        pu.open_data(file_dir=met_path, search_str='met', 
                     vars_to_import=met_vars_to_import, freq='30T',
                     start_year=2010)
        .pipe(merge_Tair_data)
        .pipe(filter_data, limits=[-20, 50], 
              drop_vars=['Tpanel_Avg', 'Ta_HMP_02_Avg', 'Ta_HMP_01_Avg'])
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
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:11:18 2020

@author: imchugh
"""

import datetime as dt
import glob
import pandas as pd
import pdb

#------------------------------------------------------------------------------
def filter_data(df, limits):

    return df.where((df >= limits[0]) & (df <= limits[1]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def interpolate_T(df, T_height):

    upper_height, lower_height = 30, 7
    dtdz = (df.Ta_upper - df.Ta_lower) / -(upper_height - lower_height)
    return df.Ta_lower + (T_height - upper_height) * dtdz
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_co2_data(dir_path, vars_to_import):

    parser = (lambda x, y:
              dt.datetime.combine(dt.datetime.strptime(x, '%d/%m/%Y').date(),
                                  dt.datetime.strptime(y, '%H:%M:%S').time()))
    df = (pd.concat([pd.read_csv(x, parse_dates = [['Date', 'Time']],
                                 date_parser=parser,
                                 usecols=vars_to_import)
                     for x in glob.glob(dir_path + '/*CO2*')]))
    df.index = df.Date_Time + dt.timedelta(seconds=1)
    return df.drop('Date_Time', axis=1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_met_data(dir_path, vars_to_import):

    df = (pd.concat([pd.read_csv(x, parse_dates = [['Date', 'Time']],
                                 usecols=vars_to_import)
                     for x in glob.glob(dir_path + '/*slow*')]))
    df.index = df.Date_Time
    names_dict = {'ps_7500_Avg': 'ps', 'Ta_HMP_01_Avg': 'Ta_lower',
                  'Ta_HMP_155_Avg': 'Ta_upper'}
    return df.drop('Date_Time', axis=1).rename(names_dict, axis=1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def stack_to_series(df, name):

    stacked_series = df.stack(dropna=False)
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def timestack_co2_data(df):

    df = (df.drop(df[~(df.index.minute % 30 == df.ValveNo)].index)
          .drop_duplicates()
          .drop('ValveNo', axis=1))
    df['ValveNo'] = df.index.minute % 30
    df['Time'] = [i - dt.timedelta(minutes = i.minute % 30) for i in df.index]
    valve_map_dict = dict(zip(sorted(df.ValveNo.unique()), heights))
    df['Height'] = df.ValveNo.apply(lambda x: valve_map_dict[x])
    df.index = pd.MultiIndex.from_frame(df[['Time', 'Height']])
    new_idx = pd.date_range(df.index.get_level_values(0)[0],
                            df.index.get_level_values(0)[-1], freq='30T')
    series = df.CO2_Avg.unstack().reindex(new_idx).stack(dropna=False)
    series.name = 'CO2'
    series.index.names = ['Time', 'Height']
    return series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBAL CONFIGURATIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
heights = [0.5, 1, 2, 3.5, 7, 12, 20, 29]
co2_vars_to_import = ['Date', 'Time', 'CO2_Avg', 'ValveNo']
met_vars_to_import = ['Date', 'Time', 'Ta_HMP_01_Avg', 'Ta_HMP_155_Avg',
                      'ps_7500_Avg']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(path):

# Construct CO2 series
    co2_series = (
        open_co2_data(path, word='CO2PROFILE',
                      vars_to_import=co2_vars_to_import)
        .pipe(timestack_co2_data)
        .pipe(filter_data, (300, 1000))
                  )

    # Get met data
    met_df = open_met_data(dir_path=path, word='slow',
                           vars_to_import=met_vars_to_import)

    # Construct temperature series
    ta_series = (
        pd.concat([interpolate_T(met_df, T_height=x) for x in heights], axis=1)
        .rename({i: x for i, x in enumerate(heights)}, axis=1)
        .pipe(stack_to_series, 'Tair')
                 )

    # Construct pressure series
    p_series = (
        pd.concat([met_df.ps.copy() for i in range(8)], axis=1,
                  ignore_index=True)
        .rename(dict(zip(range(len(heights)), heights)), axis=1)
        .pipe(stack_to_series, 'P')
                )

    return pd.concat([co2_series, ta_series, p_series], axis=1).to_xarray()
#------------------------------------------------------------------------------
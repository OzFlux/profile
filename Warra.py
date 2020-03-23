#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:35:14 2020

@author: imchugh
"""

import datetime as dt
import glob
import numpy as np
import pandas as pd

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_pressure(T_series, site_alt):

    """Estimate pressure from altitude"""

    p0 = 101325
    L = 0.0065
    R = 8.3143
    g = 9.80665
    M = 0.0289644

    A = (g * M) / (R * L)
    B = L / (T_series + 273.15)

    return (p0 * (1 - B * site_alt) ** A) / 1000
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_co2_df(df, heights):

    lag_dict = {1: 105,
                2: 90,
                3: 75,
                4: 60,
                5: 45,
                6: 30,
                7: 15,
                8: 0}

    df_list = []
    rename_dict = {i: x for i, x in enumerate(heights)}
    for i in range(8):
        valve_num = i + 1
        valve_lag = lag_dict[valve_num]
        sub_df = df.CO2_Avg.loc[df.valve_number == valve_num]
        sub_df.index = sub_df.index + dt.timedelta(seconds=valve_lag)
        df_list.append(sub_df)
    return (
        pd.concat(df_list, axis=1, ignore_index=True)
        .rename(rename_dict, axis=1)
        .pipe(resample_data)
        .pipe(stack_to_series, 'CO2')
           )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_ta_df(df, heights):

    bool_idx = (np.mod(df.index.minute, 2) == 0) & (df.index.second == 0)
    cols = sorted([x for x in df.columns if 'T_air' in x])
    rename_dict = dict(zip(cols, heights))
    return (
        df[cols][bool_idx]
        .rename(rename_dict, axis=1)
        .pipe(resample_data)
        .pipe(stack_to_series, 'Tair')
           )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(path):

    df_list = []
    for f in glob.glob(path + '/**/*')[1:]:
        df_list.append(pd.read_csv(f, parse_dates=['TIMESTAMP'],
                                   index_col=['TIMESTAMP'], skiprows=[0, 2, 3],
                                   na_values='NAN', error_bad_lines=False))
    return (
        pd.concat(df_list)
        .sort_index()
        .drop_duplicates()
        .pipe(reindex_data)
           )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reindex_data(df):

    return df.reindex(pd.date_range(df.index[0], df.index[-1], freq='15S'))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def resample_data(df):

    return df[np.mod(df.index.minute, 30) < 4].resample('30T').mean()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def stack_to_series(df, name):

    stacked_series = df.stack(dropna=False)
    stacked_series.name = name
    stacked_series.index.names = ['Time', 'Height']
    return stacked_series
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(dir_path=None):

    # Use internally defined path unless explicitly passed other
    if not dir_path: dir_path = path

    df = open_data(dir_path)
    co2_df = make_co2_df(df, heights)
    ta_df = make_ta_df(df, heights)
    ps_df = ta_df.apply(get_pressure, site_alt=150)
    ps_df.name = 'P'
    return pd.concat([co2_df, ta_df, ps_df], axis=1).to_xarray()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBALS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
path = '/home/unimelb.edu.au/imchugh/Downloads/Warra_profile/'
heights = [2, 4, 8, 16, 30, 42, 54, 70]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN PROGRAM ###
#------------------------------------------------------------------------------

if __name__ == "__main__":

    ds = get_data()
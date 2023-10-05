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
from scipy.stats import linregress
import pdb

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

CLOCK_DICT = {'first_offset': {'offset_begin': '2020-06-16 04:40:15',
                               'offset_end': '2020-08-',
                               'offset_delta': ''},
              'second_offset': ['', '2020-09-30 03:01:00']}

EDT_DICT = {'offset_begin': '2020-10-06 11:48:15',
            'offset_end': None}

T_DICT = {'offset_end': '2020-12-16 13:00:00'}

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def apply_time_correction(df):

    # return df
    error_offset_begin = '2020-06-16 04:40:15'
    error_offset_end = '2020-09-30 03:01:00'
    EDT_offset_begin = '2020-10-06 11:48:15'

    # Subset the good data prior to clock error
    unchanged_df = df.iloc[:df.index.get_loc(df.loc[error_offset_begin].name)]

    # Subset a df that corrects edt to est
    est_df = df.loc[EDT_offset_begin:]
    est_df.index -= dt.timedelta(hours=1)

    # Subset a df that corrects clock error
    fixed_df = df.loc[error_offset_begin: error_offset_end]
    delta_corr = est_df.index[0] - fixed_df.index[-1] - dt.timedelta(seconds=15)
    fixed_df.index += delta_corr

    # Concatenate
    return pd.concat([unchanged_df.copy(), fixed_df.copy(), est_df.copy()])

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def time_correction_tim(df):

    offset_dict = {'clock_offset': {'begin': '2020-06-16 04:40:15',
                                    'end': '2020-09-30 03:01:00'},
                   'aedt_offset': {'begin': '2020-10-06 11:48:15',
                                   'end': None}}

    gap_dict = {'first': {'begin': '2020-06-16 04:40:00',
                          'end': '2020-06-18 11:08:00'},
                'second': {'begin': '2020-07-11 06:08:15',
                           'end': '2020-07-11 07:14:15'},
                'third': {'begin': '2020-08-14 14:54:15',
                          'end': '2020-08-18 12:28:15'},
                'fourth': {'begin': '2020-09-22 11:30:15',
                            'end': '2020-09-22 14:00:15'}}

    # Separate 3 subsets: the preserved subset;
    #                     the subset to be corrected for clock loss;
    #                     the subset to be corrected for AEDT error
    preserve_df = (
        df.loc[df.index<offset_dict['clock_offset']['begin']].copy()
        )
    AEST_df = df.loc[offset_dict['aedt_offset']['begin']:].copy()
    AEST_df.index -= dt.timedelta(hours=1)
    df_list = [preserve_df, AEST_df]

    # Now iterate on the time error subset
    error_df = df.loc[offset_dict['clock_offset']['begin']:
                      offset_dict['clock_offset']['end']].copy()
    for i, gap in enumerate(gap_dict.keys()):
        sub_dict = gap_dict[gap]
        dt_begin = dt.datetime.strptime(sub_dict['begin'], '%Y-%m-%d %H:%M:%S')
        dt_end = dt.datetime.strptime(sub_dict['end'], '%Y-%m-%d %H:%M:%S')
        dt_delta = dt_end - dt_begin
        if i == 0:
            error_df.index = error_df.index + dt_delta
        else:
            df_list.append(error_df.loc[error_df.index<sub_dict['begin']].copy())
            error_df = error_df.loc[sub_dict['begin']:]
            error_df.index += dt_delta
    df_list.append(error_df)
    return pd.concat(df_list).sort_index()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def drop_duplicate_data(df):

    nodupes_df = df[~df.index.duplicated()]
    if not len(nodupes_df) == len(df):
        print('Warning: duplicate timestamps with different data encountered!')
    return nodupes_df
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
    # T_correct(df)
    return (
        df[cols][bool_idx]
        .rename(rename_dict, axis=1)
        .pipe(resample_data)
        .pipe(stack_to_series, 'Tair')
           )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(path):

    def open_func(f, separator=','):
        return pd.read_csv(f, sep=separator, parse_dates=['TIMESTAMP'],
                           index_col=['TIMESTAMP'], skiprows=[0, 2, 3],
                           na_values='NAN', error_bad_lines=False)

    df_list = []
    for f in sorted(glob.glob(path + '/*.dat')):
        print ('Parsing file {}'.format(f))
        try: df_list.append(open_func(f))
        except ValueError:
            print ('ValueError! File not parsed'); continue
        except OSError:
            print ('OSError! File not parsed'); continue
        #df_list.append(open_func(f, separator='\t'))

    return (
        pd.concat(df_list)
        .drop_duplicates()
        .pipe(time_correction_tim)
        .sort_index()
        .pipe(drop_duplicate_data)
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
def T_correct(df):

    """Correct temperature using regressions built from data post-replacement
       of erroneous sensors (2020-12-16 13:00); note - the lowest temperature
       sensor has no seasonal signal, so we dump it rather than correct"""

    # Make temperature dataframe
    T_list = [x for x in df.columns if 'T_air' in x]
    Tdf = df[T_list].copy()
    Tdf[(Tdf < -20) | (Tdf > 50)] = np.nan
    before_df = Tdf.loc[:T_DICT['offset_end']].dropna().copy()
    after_df = Tdf.loc[T_DICT['offset_end']:].dropna().copy()

    # Do T_air_Avg(2) correction
    after_stats = linregress(after_df['T_air_Avg(3)'], after_df['T_air_Avg(2)'])
    before_df['T_temp'] = (before_df['T_air_Avg(3)'] * after_stats.slope
                           + after_stats.intercept)
    pdb.set_trace()
    before_stats = linregress(before_df['T_air_Avg(2)'],
                              before_df['T_temp'])
    df['T_air_Avg(2)'] = (
        pd.concat([Tdf.loc[:T_DICT['offset_end'], 'T_air_Avg(2)'].iloc[:-1]
                   * before_stats.slope + before_stats.intercept,
                   Tdf.loc[T_DICT['offset_end']:, 'T_air_Avg(2)']])
        .reindex(Tdf.index)
        )

    # Do T_air_Avg(1) correction
    after_stats = linregress(after_df['T_air_Avg(3)'], after_df['T_air_Avg(1)'])
    df['T_air_Avg(1)'] = (
    pd.concat([Tdf.loc[:T_DICT['offset_end'], 'T_air_Avg(3)'].iloc[:-1]
               * after_stats.slope + after_stats.intercept,
                Tdf.loc[T_DICT['offset_end']:, 'T_air_Avg(1)']])
    .reindex(Tdf.index)
    )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(path):

    """Main function for converting raw data to profile-ready xarray format"""

    df = open_data(path)
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
heights = [2, 4, 8, 16, 30, 42, 54, 70]
site_alt = 150
#------------------------------------------------------------------------------
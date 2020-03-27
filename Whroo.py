#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 10:03:46 2020

@author: imchugh
"""

import glob
import numpy as np
import pandas as pd

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_path, word, vars_to_import):

    return (
        pd.concat([pd.read_csv(x, parse_dates=['TIMESTAMP'],
                               index_col=['TIMESTAMP'], usecols=vars_to_import,
                               skiprows=[0,2,3], na_values='NAN',
                               error_bad_lines=False)
                   for x in glob.glob(file_path + '/*{}*.dat'.format(word))])
            )
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def qc_data(df):

    for date_pair in bad_coeff_dates:
        df.loc[date_pair[0]: date_pair[1]] *= coeff_correction
    for date_pair in bad_data_dates:
        df.loc[date_pair[0]: date_pair[1]] = np.nan
    rename_dict = dict(zip(df.columns, list(reversed(heights))))
    df = df.rename(rename_dict, axis=1)[heights]
    return df.where((df > 300) & (df < 600))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def reindex_data(df):

    df = pd.concat([df.loc[:last_1min_date].resample('2T').mean(),
                    df.loc[first_2min_date:]])
    return (df[~df.index.duplicated()]
            .reindex(pd.date_range(df.index[0], df.index[-1], freq='2T')))
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
### MAIN FUNCTION ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data(path):

    # Construct CO2 series
    co2_series = (
        open_data(file_path=path, word='IRGA',
                  vars_to_import=irga_vars_to_import)
        .sort_index()
        .pipe(reindex_data)
        .pipe(qc_data)
        .pipe(resample_data)
        .pipe(stack_to_series, 'CO2')
                  )

    # Construct met df
    met_df = (
        open_data(file_path=path, word='slow', vars_to_import=met_vars_to_import)
        .drop_duplicates()
        .reindex(co2_series.index.get_level_values(0).unique())
              )

    # Construct temperature series
    ta_series = (
        pd.concat([met_df.Ta_HMP_Avg.copy() for i in range(len(heights))],
                  axis=1, ignore_index=True)
        .rename(dict(zip(np.arange(len(heights)), co2_series.index.levels[1])),
                axis=1)
        .pipe(stack_to_series, 'Tair')
                )

    # Construct pressure series
    p_series = (
        pd.concat([met_df.ps_7500_Avg.copy() for i in range(len(heights))],
                  axis=1, ignore_index=True)
        .rename(dict(zip(np.arange(len(heights)), co2_series.index.levels[1])),
                axis=1)
        .pipe(stack_to_series, 'P')
                )

    # Return xarray dataset
    return pd.concat([co2_series, ta_series, p_series], axis=1).to_xarray()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBAL CONFIGURATIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# path = '/home/unimelb.edu.au/imchugh/Downloads/Whroo_profile'
last_1min_date = '2012-02-28 12:03:00'
first_2min_date = '2012-02-28 12:10:00'
bad_coeff_dates = [['2012-06-28 11:00:00', '2012-10-17 12:50:00']]
bad_data_dates = [['2013-08-24', '2013-10-29'],
                  ['2016-10-12 12:00', '2017-05-11 11:04']]
coeff_correction = 2.5
irga_vars_to_import = ['TIMESTAMP', 'Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m',
                       'Cc_LI840_8m', 'Cc_LI840_16m', 'Cc_LI840_32m']
met_vars_to_import = ['TIMESTAMP', 'Ta_HMP_Avg', 'ps_7500_Avg']
heights = [0.5, 2, 4, 8, 16, 36]
#------------------------------------------------------------------------------
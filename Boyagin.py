#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 07:22:44 2020

@author: imchugh
"""

import numpy as np
import pandas as pd

import profile_utils as pu

#-----------------------------------------------------------------------------
### Variables ###
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
date_start = '2019-09-20 16:00'
vars_to_import = ['Cc_LI840_0_5m',  'Cc_LI840_1m',  'Cc_LI840_3m',
                  'Cc_LI840_6m', 'Cc_LI840_10m', 'Cc_LI840_16m',
                  'Cc_LI840_23m', 'Cc_LI840_30m', 'T_panel_Avg', 'P_atm_Avg']
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_path, vars_to_import):

    return (
        pd.read_csv(file_path, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
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

    # Open the data
    data_path = pu.get_path(site='Boyagin', series='Profile', state='Raw',
                            check_exists=True)
    df = pu.open_data(
        file_dir=data_path, search_str='IRGA', vars_to_import=vars_to_import,
        freq='30T'
        ).loc[date_start:]

    # Construct co2 df
    cols = [x for x in df.columns if 'Cc' in x]
    heights = [float('.'.join(x.split('_')[2:]).replace('m', ''))
               for x in cols]
    co2_series = (
        df[cols]
        .rename(dict(zip(cols, heights)), axis=1)
        .pipe(stack_to_series, 'CO2')
                 )

    # Construct temperature df
    ta_series = (
        pd.concat([df.T_panel_Avg.copy() for i in range(len(cols))], axis=1,
                  ignore_index=True)
        .rename(dict(zip(np.arange(len(cols)), co2_series.index.levels[1])),
                axis=1)
        .pipe(stack_to_series, 'Tair')
                )

    # Construct pressure df (convert to kPa from hPa - div by factor 10)
    p_series = (
        pd.concat([df.P_atm_Avg.copy() for i in range(8)], axis=1,
                  ignore_index=True)
        .rename(dict(zip(np.arange(len(cols)), co2_series.index.levels[1])),
                axis=1)
        .pipe(stack_to_series, 'P')
               ) / 10

    # Return xarray dataset
    return pd.concat([co2_series, ta_series, p_series], axis=1).to_xarray()
#-----------------------------------------------------------------------------
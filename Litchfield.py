#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 10:51:29 2020

@author: imchugh
"""

import glob
import numpy as np
import pandas as pd
import pdb

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
def open_data(dir_path, col_names):

    co2_names = [x for x in col_names if 'Cc' in x]
    df = (
       pd.concat([pd.read_excel(x, skiprows=[0,2,3], parse_dates=['TIMESTAMP'],
                                index_col='TIMESTAMP', usecols=col_names)
                  .pipe(rename_CO2, co2_names)
                  for x in glob.glob(dir_path + '/*profile*')])
       .drop_duplicates()
       .pipe(qc_data)
       )
    return df[~df.index.duplicated(keep='first')]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def qc_data(df):

    drop_bool = np.tile(True, len(df))
    for this_list in co2_drop_list:
        drop_bool &= ((df.index < this_list[0]) | (df.index > this_list[1]))
    co2_names = [x for x in df.columns if 'Cc' in x]
    other_names = [x for x in df.columns if not 'Cc' in x]
    co2_df = df[co2_names].apply(lambda x: x.where(drop_bool))
    co2_df.where((co2_df > 300) & (co2_df < 800), inplace=True)
    return pd.concat([co2_df, df[other_names]], axis=1)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def rename_CO2(df, correct_names):

    rename_dict = dict(zip([x + '_Avg' for x in correct_names], correct_names))
    return df.rename(rename_dict, axis=1)
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

    # Get base df
    df = open_data(path, col_names=variable_names)

    # Create CO2 series
    cols = [x for x in df.columns if 'Cc' in x]
    heights = [float('.'.join(x.split('_')[2:]).replace('m', ''))
               for x in cols]
    co2_series = (
        df[cols]
        .rename(dict(zip(cols, heights)), axis=1)
        .pipe(stack_to_series, 'CO2')
        )

    # Construct temperature series
    ta_series = (
        pd.concat([df.T_air_Avg.copy() for i in range(len(cols))], axis=1,
                  ignore_index=True)
        .rename(dict(zip(np.arange(len(cols)), heights)),
                axis=1)
        .pipe(stack_to_series, 'Tair')
        )

    # Construct pressure series
    ps_series = ta_series.apply(get_pressure, site_alt=150)
    ps_series.name = 'P'

    # Return xarray dataset
    return pd.concat([co2_series, ta_series, ps_series], axis=1).to_xarray()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### GLOBAL CONFIGURATIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
variable_names = ['TIMESTAMP', 'Cc_LI840_1m', 'Cc_LI840_2m', 'Cc_LI840_4m',
                  'Cc_LI840_8m', 'Cc_LI840_15m', 'Cc_LI840_30m', 'T_air_Avg']
co2_drop_list = [['2016-04-13 18:30', '2016-05-11 17:30']]
path = '/home/unimelb.edu.au/imchugh/Downloads/Litchfield_profile'
#------------------------------------------------------------------------------

if __name__ == "__main__":

    df = open_data(dir_path=path, col_names=variable_names)
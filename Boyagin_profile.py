#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 07:22:44 2020

@author: imchugh
"""

import numpy as np
import pandas as pd
import pathlib
import sys
sys.path.append(
    str(pathlib.Path(__file__).parent.parent.resolve() / 'VM_data_acquisition')
    )
from paths_manager import SitePaths
import file_io as io

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

PROFILE_FILE_NAME = 'Boyagin_CO2_prof_IRGA_avg.dat'
TEMPERATURE_FILE_NAME = 'Boyagin_EC_slow_all.dat'
DATE_START = '2023-09-05 11:10'
VARS_TO_IMPORT = ['Cc_LI840_0_5m',  'Cc_LI840_1m',  'Cc_LI840_3m',
                  'Cc_LI840_6m', 'Cc_LI840_10m', 'Cc_LI840_16m',
                  'Cc_LI840_23m', 'Cc_LI840_30m', 'T_panel_Avg', 'P_atm_Avg']

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASS INSTANTIATIONS ###
#------------------------------------------------------------------------------

paths = SitePaths(site='Boyagin')

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
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

# #------------------------------------------------------------------------------
def return_data():

    """Main function for converting raw data to profile-ready xarray format"""

    # Get dataframe and grab the mean of the 2
    profile_df = (
        io.get_data(
            file=paths.local_data.profile_raw / PROFILE_FILE_NAME,
            file_type='TOA5',
            usecols=VARS_TO_IMPORT
            )
        .loc[DATE_START:]
        .drop('TIMESTAMP', axis=1)
        )

    # Resample dataframe (use mean of 28-30 and 0-2 minute samples)
    profile_df = (
        profile_df[np.mod(profile_df.index.minute, 30) < 4]
        .resample('30T')
        .mean()
        )

    # Construct co2 df
    cols = [x for x in profile_df.columns if 'Cc' in x]
    heights = [float('.'.join(x.split('_')[2:]).replace('m', ''))
                for x in cols]
    co2_series = (
        profile_df[cols]
        .rename(dict(zip(cols, heights)), axis=1)
        .pipe(stack_to_series, 'CO2')
        )

    # # Construct temperature df
    # ta_series = (
    #     pd.concat(
    #         [profile_df.T_panel_Avg.copy() for i in range(len(cols))], axis=1,
    #         ignore_index=True
    #         )
    #     .rename(
    #         dict(zip(np.arange(len(cols)), co2_series.index.levels[1])),
    #         axis=1
    #         )
    #     .pipe(stack_to_series, 'Tair')
    #     )

    # Construct pressure df (convert to kPa from hPa - div by factor 10)
    p_series = (
        pd.concat(
            [profile_df.P_atm_Avg.copy() for i in range(8)], axis=1,
            ignore_index=True
            )
        .rename(
            dict(zip(np.arange(len(cols)), co2_series.index.levels[1])),
            axis=1)
        .pipe(stack_to_series, 'P')
        ) / 10

    # Temporarily construct the temperature data from a different dataset
    temp_df = (
        io.get_data(file=paths.local_data.flux_slow / TEMPERATURE_FILE_NAME)
        .loc[DATE_START:]
        )

    # Construct temperature df
    ta_series = (
        pd.concat(
            [temp_df.Ta_HMP_Avg.copy() for i in range(len(cols))], axis=1,
            ignore_index=True
            )
        .rename(
            dict(zip(np.arange(len(cols)), co2_series.index.levels[1])),
            axis=1
            )
        .pipe(stack_to_series, 'Tair')
        )

    # Concatenate xarray dataset, assign variable attrs and return
    ds = pd.concat([co2_series, ta_series, p_series], axis=1).to_xarray()
    ds.CO2.attrs = {'units': 'umol/mol'}
    ds.Tair.attrs = {'units': 'degC'}
    ds.P.attrs = {'units': 'kPa'}
    return ds
#------------------------------------------------------------------------------

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 07:22:44 2020

@author: imchugh
"""

import numpy as np
import pandas as pd
import pdb

import profile_utils as pu

#-----------------------------------------------------------------------------
### CONSTANTS ###
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
DATE_START = '2019-09-20 16:00'
VARS_TO_IMPORT = ['Cc_LI840_0_5m',  'Cc_LI840_1m',  'Cc_LI840_3m',
                  'Cc_LI840_6m', 'Cc_LI840_10m', 'Cc_LI840_16m',
                  'Cc_LI840_23m', 'Cc_LI840_30m', 'T_panel_Avg', 'P_atm_Avg']
#-----------------------------------------------------------------------------

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

#------------------------------------------------------------------------------
def get_data(path):

    """Main function for converting raw data to profile-ready xarray format"""

    # Get dataframe
    df = pu.open_data(
        file_dir=path, search_str='IRGA', vars_to_import=VARS_TO_IMPORT,
        freq='2T'
        ).loc[DATE_START:]

    # Resample dataframe (use mean of 28-30 and 0-2 minute samples)
    df = df[np.mod(df.index.minute, 30) < 4].resample('30T').mean()

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
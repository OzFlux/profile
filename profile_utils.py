# -*- coding: utf-8 -*-
"""
Created on Tue May 18 17:47:40 2021

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import datetime as dt
import logging
import pandas as pd
import pathlib
import pdb

#-----------------------------------------------------------------------------
SERIES_LIST = ['flux', 'profile']
STATE_LIST = ['raw', 'processed']
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def get_path(site, series, state, check_exists=False):

    """Use initialisation file to extract data path for site, 
       series and state"""    

    config_map = {'raw': 'RAW_DATA_PATH', 'processed': 'PROCESSED_DATA_PATH'}
    if not series in SERIES_LIST:
            raise KeyError('series arg must be either {}'
                           .format(' or '.join(SERIES_LIST)))
    if not state in STATE_LIST:
            raise KeyError('state arg must be either {}'
                           .format(' or '.join(STATE_LIST)))
    sub_path = config_map[state]
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths.ini')
    base_data_path = (
        pathlib.Path(config['DEFAULT']['data_path'].replace('<site>', site))
        )
    sub_data_path = config[config_map[state]][series]
    out_path = base_data_path / sub_data_path
    if not check_exists: return out_path
    if not out_path.exists():
        raise FileNotFoundError('path does not exist')
    else:
        return out_path
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_dir, search_str, freq, vars_to_import=None, start_year=None):

    file_list = list(file_dir.rglob('*{}*'.format(search_str)))
    if  not file_list:
        raise FileNotFoundError('No files found containing searchphrase {}'
                                  .format(search_str))
    df_list = []
    for f in file_list:
        try:
            df_list.append(
                pd.read_csv(f, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                            index_col=['TIMESTAMP'], na_values='NAN',
                            on_bad_lines='skip'))
        except ValueError:
            continue
    return_df = pd.concat(df_list)
    return_df = return_df[~return_df.index.isnull()]
    if start_year:
        return_df = return_df[return_df.index.year >= start_year]
    if vars_to_import:
        valid_list = [x for x in vars_to_import if x in return_df.columns]
        invalid_list = [x for x in vars_to_import if not x in return_df.columns]
        if invalid_list:
            logging.warning('The following values were missing from the '
                            'concatenated data and will be ignored: {}'
                            .format(', '.join(invalid_list)))
        return_df = return_df[valid_list]
    return_df.sort_index(inplace=True)
    return_df = return_df[~return_df.index.duplicated(keep = 'first')]
    return_df = return_df[~return_df.index.isnull()]
    new_index = pd.date_range(return_df.index[0], return_df.index[-1], freq=freq)
    return return_df.reindex(new_index)
#------------------------------------------------------------------------------
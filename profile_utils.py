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
def get_path(state, series, site, check_exists=False):
    
    state_dict = {'raw': 'raw_data_read_path', 
                  'processed': 'processed_data_write_path'}
    proc_type = state_dict[state]
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths.ini')
    out_path = pathlib.Path(config[proc_type][series].format(site))
    if not check_exists: return out_path
    if not out_path.exists(): raise FileNotFoundError('path does not exist')
    else: return out_path
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_dir, search_str, freq, vars_to_import=None, start_year=None):
    
    file_list = list(file_dir.rglob('*{}*.dat'.format(search_str)))
    if  not file_list: 
        raise FileNotFoundError('No files found containing searchphrase {}'
                                  .format(search_str))
    df_list = []
    for f in file_list:
        try:
            df_list.append(
                pd.read_csv(f, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                            index_col=['TIMESTAMP'], na_values='NAN', 
                            error_bad_lines=False))
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
    return_df.sort_index(inplace = True)
    return_df = return_df[~return_df.index.duplicated(keep = 'first')]
    return_df = return_df[~return_df.index.isnull()]
    new_index = pd.date_range(return_df.index[0], return_df.index[-1], freq=freq)
    return return_df.reindex(new_index)
#------------------------------------------------------------------------------
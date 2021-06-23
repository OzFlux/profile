# -*- coding: utf-8 -*-
"""
Created on Tue May 18 17:47:40 2021

@author: jcutern-imchugh
"""

from configparser import ConfigParser
import pandas as pd
import pathlib
import pdb

#-----------------------------------------------------------------------------
def get_path(state, series, site, check_exists=False):
    
    state_dict = {'raw': 'raw_data_read_path', 
                  'processed': 'processed_data_write_path'}
    # series_list = ['slow_flux', 'fast_flux', 'profile']
    proc_type = state_dict[state]
    config = ConfigParser()
    config.read(pathlib.Path(__file__).parent / 'paths.ini')
    out_path = pathlib.Path(config[proc_type][series].format(site))
    if not check_exists: return out_path
    if not out_path.exists(): raise FileNotFoundError('path does not exist')
    else: return out_path
#-----------------------------------------------------------------------------

#------------------------------------------------------------------------------
def open_data(file_dir, search_str, vars_to_import, freq):

    file_list = list(file_dir.glob('*{}*.dat'.format(search_str)))
    if  not file_list: 
        raise FileNotFoundError('No files found containing searchphrase {}'
                                  .format(search_str))
    df_list = []
    for f in file_list:
        try:
            df_list.append(
                pd.read_csv(f, parse_dates=['TIMESTAMP'], skiprows=[0, 2, 3],
                            index_col=['TIMESTAMP'], usecols=vars_to_import,
                            na_values='NAN', error_bad_lines=False,
                            dtype={x: 'float' for x in vars_to_import}))
        except ValueError:
            continue
    return_df = pd.concat(df_list)
    return_df.sort_index(inplace = True)
    return_df = return_df[~return_df.index.duplicated(keep = 'first')]
    new_index = pd.date_range(return_df.index[0], return_df.index[-1], freq=freq)
    return return_df.reindex(new_index)
#------------------------------------------------------------------------------
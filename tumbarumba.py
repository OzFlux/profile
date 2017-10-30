import os
import pandas as pd
import pdb
import xlrd

#------------------------------------------------------------------------------    
def get_file_list(path, word):
    f_list = filter(lambda x: word in x, os.listdir(path))
    return sorted(map(lambda x: os.path.join(path, x), f_list))
#------------------------------------------------------------------------------ 

#------------------------------------------------------------------------------    
def make_names_dict(col_names, search_obj):
    if isinstance(search_obj, dict):
        searchstr = search_obj.keys()[0]
        replacestr = search_obj.values()[0] 
    else:
        searchstr = search_obj
        replacestr = search_obj
    names = filter(lambda x: searchstr in x, col_names)
    return dict(zip(names, ['{0}_{1}m'.format(replacestr, str(x)) 
                            for x in heights]))
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def open_data(file_path, sheet_name, header_row, search_obj, 
              ancillary_dict = None):
    book = xlrd.open_workbook(file_path)
    sheet = book.sheet_by_name(sheet_name)
    date_time = map(lambda x: xlrd.xldate_as_datetime(x, 
                                                      datemode = book.datemode), 
                    sheet.col_values(0, header_row + 1))
    col_names = sheet.row_values(9)
#    for var in 
    names_dict = make_names_dict(col_names, search_obj)
    df = pd.DataFrame(index = date_time)
    for old_name in sorted(names_dict.keys()):
        new_name = names_dict[old_name]
        data_idx = col_names.index(old_name)
        df[new_name] = pd.to_numeric(sheet.col_values(data_idx, 
                                                      header_row + 1),
                                     errors = 'coerce')    
    if not ancillary_dict is None:
        for old_name in ancillary_dict.keys():
            new_name = ancillary_dict[old_name]
            data_idx = col_names.index[old_name]
            df[new_name] = pd.to_numeric(sheet.col_values(data_idx, 
                                                          header_row + 1),
                                         errors = 'coerce')
    return df
#------------------------------------------------------------------------------    

# standard initialisations    
path = '/home/ian/Desktop'
f_list = os.listdir(path)
heights = [1, 5, 10, 18, 26, 34, 42, 56, 70]

# CO2 data sheet initialisations
CO2_file_searchstr = 'gasprof'
CO2_sheet_name = 'gas_data'
CO2_header_row = 9
CO2_var_searchstr = 'CO2'

# Met data sheet initialisations
met_file_searchstr = 'slow'
met_sheet_name = 'slow_15min'
met_header_row = 10
met_var_searchstr = {'TC': 'Tair'}
ancillary_var_dict = {} 

# Get irga dataframe and resample
irga_fp_list = get_file_list(path, CO2_file_searchstr)
irga_df = pd.concat(map(lambda x: open_data(x,
                                            CO2_sheet_name,
                                            CO2_header_row,
                                            CO2_var_searchstr), irga_fp_list))
irga_df = irga_df.resample('5T').mean().resample('30T').pad()

# Get met dataframe
met_fp_list = get_file_list(path, met_file_searchstr)
met_df = pd.concat(map(lambda x: open_data(x,
                                           met_sheet_name,
                                           met_header_row,
                                           met_var_searchstr), met_fp_list))


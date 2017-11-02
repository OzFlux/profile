import os
import pandas as pd
import pdb
import xlrd

#------------------------------------------------------------------------------
def align_data(df_list):
    begin_list, end_list = [], []
    for df in df_list:
        begin_list.append(df.index[0])
        end_list.append(df.index[-1])
        df.sort_index(inplace = True)
        df.drop_duplicates(inplace = True)
        df = df[~df.index.duplicated(keep = 'first')]
    return df_list[0].join(df_list[1])    
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------    
def get_file_list(path, word):
    f_list = filter(lambda x: word in x, os.listdir(path))
    return sorted(map(lambda x: os.path.join(path, x), f_list))
#------------------------------------------------------------------------------ 

#------------------------------------------------------------------------------    
def make_names_dict(col_names, pair):
    searchstr = pair[0]
    replacestr = pair[1]
    names = filter(lambda x: searchstr in x, col_names)
    if len(names) == 0:
        raise IndexError('Could not find any variables containing substring {}'
                         .format(searchstr))
    try:
        assert len(names) == len(heights)
        return dict(zip(names, ['{0}_{1}m'.format(replacestr, str(x)) 
                                for x in heights]))
    except AssertionError:
        return {names[0]: replacestr}
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def open_data(file_path, sheet_name, header_row, vars_dict):
    print file_path
    book = xlrd.open_workbook(file_path)
    sheet = book.sheet_by_name(sheet_name)
    date_time = map(lambda x: xlrd.xldate_as_datetime(x, 
                                                      datemode = book.datemode), 
                    sheet.col_values(0, header_row + 1))
    col_names = sheet.row_values(9)
    df = pd.DataFrame(index = date_time)
    for keyval_tuple in vars_dict.items():
        try:
            names_dict = make_names_dict(col_names, keyval_tuple)
            for old_name in sorted(names_dict.keys()):
                new_name = names_dict[old_name]
                data_idx = col_names.index(old_name)
                df[new_name] = pd.to_numeric(sheet.col_values(data_idx, 
                                                              header_row + 1),
                                             errors = 'coerce')    
        except IndexError:
            continue
    return df
#------------------------------------------------------------------------------    

# standard initialisations    
path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/Tumbarumba'
f_list = os.listdir(path)
heights = [1, 5, 10, 18, 26, 34, 42, 56, 70]

# CO2 data sheet initialisations
CO2_file_searchstr = 'gasprof'
CO2_sheet_name = 'gas_data'
CO2_header_row = 9
CO2_var_searchstr = {'CO2': 'CO2',
                     'TUC': 'CO2'}

# Met data sheet initialisations
met_file_searchstr = 'slow'
met_sheet_name = 'slow_15min'
met_header_row = 10
met_var_searchstr = {'TC': 'Tair',
                     'P_PTB101': 'ps'}

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
df = align_data([irga_df, met_df])

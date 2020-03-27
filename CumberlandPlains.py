#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:11:18 2020

@author: imchugh
"""

import datetime as dt
import glob
import pandas as pd

def open_data(dir_path, word, vars_to_import):

    parser = (lambda x, y:
              dt.datetime.combine(dt.datetime.strptime(x, '%d/%m/%Y').date(),
                                  dt.datetime.strptime(y, '%H:%M:%S').time()))
    df = (pd.concat([pd.read_csv(x, parse_dates = [['Date', 'Time']],
                                 date_parser=parser,
                                 usecols=vars_to_import)
                     for x in glob.glob(dir_path + '/*CO2*')]))
    df.index = df.Date_Time + dt.timedelta(seconds=1)
    return df.drop('Date_Time', axis=1)#.pipe(timestack_data)

def timestack_data(df):

    df = (df.drop(df[~(df.index.minute%30 == df.ValveNo)].index)
          .drop_duplicates()
          .drop('ValveNo', axis=1))
    flat_idx = [i-dt.timedelta(minutes=i.minute%30) for i in df.index]
    valve = df.index.minute%30
    df.index = pd.MultiIndex.from_tuples(list(zip(flat_idx, valve)))
    return df

path = '/home/unimelb.edu.au/imchugh/Downloads/CumberlandPlains_profile'
heights = [0.5, 1, 2, 3.5, 7, 12, 20, 29]

co2_df = open_data(path, word='CO2PROFILE',
                   vars_to_import=['Date', 'Time', 'CO2_Avg', 'ValveNo'])
# def cumberland_plains():

#     def interpolate_T(T_series_lower, T_series_upper, T_height):

#         T1_height = 7
#         T2_height = 30
#         dtdz = (T_series_upper - T_series_lower) / (T2_height - T1_height)

#         return (T_height - T1_height) * dtdz + T_series_lower

#     max_CO2 = 1000
#     min_CO2 = 300
#     target_var = 'CO2_Avg'
#     missing_data_float = -9999
#     input_profile_file_path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/CumberlandPlains/EFS_S00_CO2PROFILE_R_2017.csv'
#     input_met_data_path = '/home/ian/ownCloud_dav/Shared/Monash-OzFlux/Profile_data/CumberlandPlains/EddyFlux_slow_met_2017.csv'

#     # Get data and create index
#     df = pd.read_csv(input_profile_file_path)
#     df.index = [dt.datetime.combine(dt.datetime.strptime(
#                                         df.Date[i], '%d/%m/%Y').date(),
#                                     dt.datetime.strptime(
#                                         df.Time[i], '%H:%M:%S').time())
#                 for i in xrange(len(df))]

#     # Remove missing_data_values and replace with NaN
#     df[target_var] = np.where(df[target_var] == missing_data_float,
#                               np.nan,
#                               df[target_var])
#     total_obs = len(df)
#     missing_obs = total_obs - len(df[target_var].dropna())
#     print ('{0}% of records contain no data'
#            .format(str(round(missing_obs / float(total_obs),1))))

#     # Remove data that is outside specified range and replace with NaN
#     df[target_var] = np.where((df[target_var] < min_CO2)|
#                               (df[target_var] > max_CO2),
#                               np.nan,
#                               df[target_var])
#     dropped_obs = len(df[target_var].dropna()) - missing_obs
#     print ('{0}% of records contain data outside range limits'
#            .format(str(round(dropped_obs / float(total_obs),1))))

#     # Remove irregular time stamps then align all to half-hour (+59 seconds!!!)
#     startdate_lst = []
#     enddate_lst = []
#     series_list = []
#     valve_list = [1, 2, 3, 4, 5, 6, 7, 8]
#     heights_list = [0.5, 1, 2, 3.5, 7, 12, 20, 29]
#     for i, valve in enumerate(valve_list):
#         new_name = 'CO2_{0}m'.format(str(heights_list[i]))
#         expected_minute = valve - 1
#         temp_s = df[target_var][df['ValveNo'] == valve].copy()
#         index = [i for i in temp_s.index if i.minute %30 != expected_minute]
#         if not len(index) == 0:
#             print ('{0} instances of inconsistent time step found for valve {1} '
#                    '({2}); removed!'
#                    .format(str(len(index)),
#                            str(valve),
#                            ','.join([str(i) for i in index])))
#             temp_s.drop(index, axis = 0, inplace = True)
#         new_index = [i - dt.timedelta(minutes = expected_minute,
#                                       seconds = 59) for i in temp_s.index]
#         temp_s.index = new_index
#         temp_s.name = new_name
#         startdate_lst.append(temp_s.index[0])
#         enddate_lst.append(temp_s.index[-1])
#         series_list.append(temp_s)

#     # Sort the start and end date lists and create a universal date range to be applied to all valve series'
#     startdate_lst.sort()
#     enddate_lst.sort()
#     new_index = pd.date_range(startdate_lst[0], enddate_lst[-1], freq='30T')

#     # Reindex, write to dataframe and return
#     for series in series_list:
#         series = series.reindex(new_index)
#     output_df = pd.concat(series_list, axis = 1)

#     # Import met data and align time index with profile data
#     met_df = pd.read_csv(input_met_data_path)
#     met_df.index = [dt.datetime.combine(dt.datetime.strptime(
#                                             met_df.Date[i], '%d/%m/%Y').date(),
#                                         dt.datetime.strptime(
#                                             met_df.Time[i], '%H:%M:%S').time())
#                     for i in xrange(len(met_df))]
#     met_df = met_df[['Ta_HMP_01_Avg', 'Ta_HMP_155_Avg', 'ps_7500_Avg']]
#     met_df.columns = ['Tair_lower', 'Tair_upper', 'ps']
#     met_df = met_df.reindex(output_df.index)

#     # Interpolate the temperature data to the same heights as the CO2
#     # measurements
#     for this_height in heights_list:
#         T_name = 'Tair_{0}m'.format(str(this_height))
#         output_df[T_name] = interpolate_T(met_df.Tair_lower, met_df.Tair_upper,
#                                           this_height)
#     output_df['ps'] = met_df.ps

#     return output_df
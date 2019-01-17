#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May 22 18:33:19 2018

@author: ian
"""

import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pdb

class profile(object):
    
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def __init__(self, df, use_T_var = None, use_P_var = None, site_alt = None):
        '''
        Docstring here!
        '''
        self.df = df
        self.interval = self._get_data_interval_mins()
        self._use_T_var = use_T_var
        self._use_P_var = use_P_var
        self.site_alt = site_alt    
        self.CO2_names = self.get_names()
        self.n_levels = len(self.CO2_names)
        self.T_names = self._check_integrity(self._use_T_var, 'Tair')
        try:
            self.P_names = self._check_integrity(self._use_P_var, 'ps')
        except KeyError:
            self.P_names = ['ps']
            self._make_ps()
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------            
    def _check_integrity(self, use_var, name_str):    
        if use_var: 
            assert isinstance(use_var, str)
            assert use_var in self.df.columns
            return [use_var]
        else:
            names = self.get_names(name_str)
            if len(names) == 0:
                raise KeyError('No variables found in dataframe!')
            elif len(names) == 1: 
                return names
            elif len(names) == self.n_levels:
                assert self.get_heights() == self.get_heights(name_str)
                return names
            else:
                raise RuntimeError('Wrong number of variables; '
                                   'must be same as CO2 or 1 (got {})'
                                   .format(str(len(names))))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _get_data_interval_mins(self):
        
        freq = pd.infer_freq(self.df.index)
        if not freq: raise RuntimeError('Time series non-continuous... exiting')
        if freq == 'H':
            interval = 60
        if 'T' in freq:
            interval = int(filter(lambda x: x.isdigit(), 
                                  pd.infer_freq(self.df.index)))
        assert interval % 30 == 0
        return interval
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_diel_storage_average(self, write_to_filepath = None):
        
        # How would this deal with 90 minutes? - CRASH!!!
        df = self.get_storage_time_series()
        diel_df = df.groupby([lambda x: x.hour, lambda y: y.minute]).mean()
        if 60 / self.interval > 1:
            diel_df.index = np.arange(48) / 2.0
        else:
            diel_df.index = diel_df.index.get_level_values(0)
        if write_to_filepath: self.write_to_file(write_to_filepath)
        return diel_df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_heights(self, search_str = 'CO2'):
        
        names_list = self.get_names(search_str)
        return sorted(map(lambda x: float(x.split('_')[1][:-1]), names_list))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_layer_depths(self):
        
        heights_list = self.get_heights()
        return list(np.array(heights_list - np.array([0] + heights_list[:-1])))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_layer_names(self, prefix = 'CO2'):
        
        heights_list = [0] + self.get_heights()
        return map(lambda x: '{0}_{1}-{2}m'.format(prefix,
                                                   str(heights_list[x - 1]), 
                                                   str(heights_list[x])), 
                   range(1, len(heights_list)))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_layer_series(self):
        
        molar_df = self.get_molar_density_time_series()
        layer_names = self.get_layer_names()
        data_list = []
        for i in range(self.n_levels):
            level_name = self.CO2_names[i]
            layer_name = layer_names[i]
            if i == 0:
                s = molar_df[level_name]
            else:
                prev_level_name = self.CO2_names[i - 1]
                s = (molar_df[level_name] + molar_df[prev_level_name]) / 2
            s.name = layer_name
            data_list.append(s)
        return pd.concat(data_list, axis = 1)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def get_molar_density_time_series(self):
        
        CO2_list = self.CO2_names
        if len(self.T_names) == 1: 
            T_list = self.T_names * self.n_levels
        else:
            T_list = self.T_names
        if len(self.P_names) == 1:
            P_list = self.P_names * self.n_levels
        else:
            P_list = self.P_names
        var_set = zip(CO2_list, T_list, P_list)
        data_list = []
        for this_set in var_set:
            co2, T, P = this_set[0], this_set[1], this_set[2]
            try:
                molar_density_series = (self.df[P] * 10**3 / 
                                        (8.3143 * (273.15 + self.df[T])))
            except:
                pdb.set_trace()
            CO2_molar_density_series = (molar_density_series * 
                                        self.df[co2] * 10**-6)
            CO2_molar_density_series.name = co2
            data_list.append(CO2_molar_density_series)
        return pd.concat(data_list, axis = 1)
    #--------------------------------------------------------------------------    

    #--------------------------------------------------------------------------    
    def get_names(self, search_str = 'CO2'):
        
        names_list = sorted(filter(lambda x: search_str in x, self.df.columns))
        if len(names_list) == 1: return names_list
        numbers_list = map(lambda x: float(x.split('_')[1][:-1]), names_list)
        index_arr = np.argsort(np.array(numbers_list))
        return list(np.array(names_list)[index_arr])
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------    
    def get_storage_time_series(self, write_to_filepath = None):
        
        layer_df = self.get_layer_series()
        diff_df = (layer_df - layer_df.shift())
        layer_names = self.get_layer_names()
        mult_dict = dict(zip(layer_names, self.get_layer_depths()))
        name_dict = dict(zip(layer_names, self.get_layer_names('Sc')))
        data_dict = []
        for level_name in layer_names:
            new_name = name_dict[level_name]
            s = (diff_df[level_name] / (self.interval * 60) * 10**6 *
                 mult_dict[level_name])
            s.name = new_name
            data_dict.append(s)
        output_df = pd.concat(data_dict, axis = 1)
        output_df['Sc_total'] = output_df.sum(axis = 1)
        nans = np.isnan(output_df[output_df.columns[:-1]]).sum(axis=1) != 0
        output_df.loc[nans, 'Sc_total'] = np.nan
        if write_to_filepath: self.write_to_file(write_to_filepath)
        return output_df
    #--------------------------------------------------------------------------    
    
    #--------------------------------------------------------------------------
    def plot_diel_average(self):
    
        df = self.get_diel_storage_average()
        vars_list = list(df.columns)
        vars_list.remove('Sc_total')
        strip_vars_list = [var.split('_')[1] for var in vars_list]
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
        fig.patch.set_facecolor('white')
        colour_idx = np.linspace(0, 1, len(vars_list))
        ax.set_xlim([0, 24])
        ax.set_xticks([0,4,8,12,16,20,24])
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.set_xlabel('$Time$', fontsize = 18)
        ax.set_ylabel('$S_c\/(\mu mol\/CO_2\/m^{-2}\/s^{-1})$', fontsize = 18)
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        for i, var in enumerate(vars_list):
            color = plt.cm.cool(colour_idx[i])
            plt.plot(df.index, df[var], label = strip_vars_list[i], color = color)
        plt.plot(df.index, df.Sc_total, label = 'Total', color = 'grey')
        plt.legend(loc=[0.65, 0.18], frameon = False, ncol = 2)    
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def plot_time_series(self):
        
        df = self.get_storage_time_series()
        vars_list = list(df.columns)
        vars_list.remove('Sc_total')
        strip_vars_list = [var.split('_')[1] for var in vars_list]
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
        fig.patch.set_facecolor('white')
        colour_idx = np.linspace(0, 1, len(vars_list))
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.set_xlabel('$Date$', fontsize = 18)
        ax.set_ylabel('$S_c\/(\mu mol\/CO_2\/m^{-2}\/s^{-1})$', fontsize = 18)
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        plt.plot(df.index, df.Sc_total, label = 'Total', color = 'grey', 
                 alpha = 0.5)
        for i, var in enumerate(vars_list):
            color = plt.cm.cool(colour_idx[i])
            plt.plot(df.index, df[var], label = strip_vars_list[i], 
                     color = color, alpha = 0.4)
        plt.legend(loc='lower left', frameon = False, ncol = 2)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _make_ps(self):
        
        if not self.site_alt:
            print ('Warning: there are no pressure data available in the raw '
                   'data file and a site altitude has not been specified; '
                   'standard sea level pressure will be used for subsequent '
                   'calculations but may result in substantial storage '
                   'underestimation for high altitude sites (by a factor of '
                   '1-p/p0!')
            self.df['ps'] = 101.3
        else:
            p0 = 101325
            L = 0.0065
            R = 8.3143
            T0 = 288.15
            g = 9.80665
            M = 0.0289644
            A = (g * M) / (R * L)
            B = L / T0
            p = (p0 * (1 - B * self.site_alt) ** A) / 1000                          
            self.df['ps'] = p             
        return
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def write_to_file(self, file_path):

        path = os.path.split(file_path)[0]
        assert os.path.isdir(path)
        df = self.get_storage_time_series()
        df.to_csv(file_path, index_label = 'Datetime')
    #--------------------------------------------------------------------------
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 13:24:26 2017

@author: ian
"""

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy as np
import os
import pandas as pd
import pdb

def read_func(path, drop_dates = False):
    df = pd.read_csv(path)
    df.index = pd.to_datetime(df.Datetime)
    df.drop('Datetime', axis = 1, inplace = True)
    if drop_dates:
        df = df.loc['2016-07-01 12:00:00':]
    sub_df = df.groupby([lambda x: x.hour, lambda y: y.minute]).mean()
    sub_df.index = np.linspace(0, 23.5, 48)
    return sub_df['Sc_total']

data_dir = '/home/ian/Data/Profile'
sites_dict = {'cup': 'Cumberland Plains',
              'howardsprings': 'Howard Springs',
              'robson': 'Robsons Creek',
              'tumbarumba': 'Tumbarumba',
              'warra': 'Warra',
              'whroo': 'Whroo',
              'wombat': 'Wombat State Forest'}

site_list = sorted(sites_dict.keys())
result_df = pd.DataFrame(columns = site_list)
for site in sorted(sites_dict.keys()):
    fp_str = os.path.join(data_dir, '{}.csv'.format(site))
    if not site == 'tumbarumba':
        result_df[site] = read_func(fp_str)
    else:
        result_df[site] = read_func(fp_str, True)
        
amp_order = (result_df.max() - result_df.min()).sort_values().index.values

linestyles_list = ['-', ':', '--', '-.', '-', ':', '--']
color_list = ['black', 'blue', 'green', 'orange', 'magenta', 'cyan', 'brown']
colour_idx = np.linspace(0, 1, len(result_df.columns))
font = FontProperties()
font.set_family('sans serif')
font.set_style('italic')
fig, ax = plt.subplots(1, 1, figsize = (12, 8))
fig.patch.set_facecolor('white')
ax.xaxis.set_ticks_position('bottom')
ax.yaxis.set_ticks_position('left')
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.tick_params(axis = 'x', labelsize = 14)
ax.tick_params(axis = 'y', labelsize = 14)
ax.set_xlim([0, 23.5])
ax.set_xticks([0, 3, 6, 9, 12, 15, 18, 21, 24])
ax.set_xlabel('Time (hrs)', fontsize = 18, fontproperties = font)
ax.set_ylabel('$S_c\/(\mu mol\/m^{-2}\/s^{-1}$)', fontsize = 18, 
              fontproperties = font)
ax.axhline(0, color = 'grey')
for i, site in enumerate(amp_order):
    site_amp = round(result_df[site].max() - result_df[site].min(), 1)
    figs_str = '{0} ({1})'.format(sites_dict[site], str(site_amp))
    plt.plot(result_df.index, result_df[site].rolling(window = 3, 
                                                      center = True).mean(), 
             label = figs_str,
             color = plt.cm.spectral(colour_idx[i]))#, ls = linestyles_list[i])
             #color = color_list[i])
ax.legend(loc = (0.65, 0.1), frameon = False, fontsize = 14)


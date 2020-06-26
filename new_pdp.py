#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 16:41:00 2020

@author: imchugh
"""

import configparser
import matplotlib.pyplot as plt
import numpy as np
import os
import xarray as xr

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

class profile():

    def __init__(self, ds, site='Unknown'):

        self.dataset = ds
        self.site = site

    def get_heights(self):

        """Get gas sampling intake array heights in m"""

        return list(self.dataset.Height.data)

    def get_layer_depths(self):

        """Get distance in metres between intakes"""

        heights = self.get_heights()
        return np.array(heights) - np.array([0] + heights[:-1])

    def _get_layer_names(self):

        """Get name suffixes for layers"""

        layer_elmts = [0] + self.get_heights()
        str_layer_elmts = ([str(int(x)) if x==int(x) else str(x)
                           for x in layer_elmts])
        return ['{0}-{1}m'.format(str_layer_elmts[i-1], str_layer_elmts[i])
                for i in range(1, len(str_layer_elmts))]

    def get_CO2_density(self, as_df=False):

        """Calculate the density in mgCO2 m^-3 from n=PV/RT"""

        CO2_const = 8.3143 / 44
        da = (self.dataset.P * 1000 /
              (CO2_const * (self.dataset.Tair + 273.15))
              * self.dataset.CO2 / 10**3)
        da.name = 'CO2_density'
        if not as_df: return da
        return _get_dataframe(da)

    def get_CO2_density_as_layers(self, as_df=False):

        """Get the layer mean CO2 density (lowest layer is assumed to be
           constant between ground and lowermost intake, other layers are
           simple mean of upper and lower bounds of layer)"""

        density_da = self.get_CO2_density()
        da_list = []
        da_list.append(density_da.sel(Height=density_da.Height[0])
                       .reset_coords('Height', drop=True))
        for i in range(1, len(density_da.Height)):
            da_list.append(density_da.sel(Height=density_da.Height[i-1: i+1])
                           .mean('Height'))
        layer_da = xr.concat(da_list, dim='Layer')
        layer_da['Layer'] = self._get_layer_names()
        layer_da = layer_da.transpose()
        if not as_df: return layer_da
        return _get_dataframe(layer_da)

    def get_delta_CO2_storage(self, as_df=False):

        """Get storage term"""

        layer_da = self.get_CO2_density_as_layers()
        layer_da = layer_da / 44 * 10**3 # Convert g m^-3 to umol m^-3
        diff_da = layer_da - layer_da.shift(Time=1) # Difference
        diff_da = diff_da / 1800 # Divide by time interval
        # Scale by layer depth
        depth_scalar = xr.DataArray(self.get_layer_depths(), dims='Layer')
        depth_scalar['Layer'] = diff_da.Layer.data
        diff_da = diff_da * depth_scalar
        diff_da['Layer'] = ['dCO2s_{}'.format(x) for x in diff_da.Layer.data]
        diff_da.name = 'delta_CO2_storage'
        if not as_df: return diff_da
        return _get_dataframe(diff_da)

    def get_summed_delta_CO2_storage(self, as_df=False):

        """Get storage term summed over all layers"""

        da = self.get_delta_CO2_storage()
        if not as_df: return da.sum('Layer', skipna=False)
        return da.sum('Layer', skipna=False).to_dataframe()

    def plot_diel_storage_mean(self):

        """Plot the diel mean"""

        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_sum'] = df.sum(axis=1, skipna=False)
        diel_df = df.groupby([df.index.hour, df.index.minute]).mean()
        diel_df.index = np.arange(len(diel_df)) / 2
        diel_df.index.name = 'Time'
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
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
        ax.axhline(0, color='black', ls=':')
        colour_idx = np.linspace(0, 1, len(diel_df.columns[:-1]))
        labs = [x.split('_')[1] for x in diel_df.columns]
        for i, var in enumerate(diel_df.columns[:-1]):
            color = plt.cm.cool(colour_idx[i])
            ax.plot(diel_df[var], label = labs[i], color = color)
        ax.plot(diel_df[diel_df.columns[-1]], label = labs[-1],
                color='grey')
        ax.legend(loc=[0.65, 0.18], frameon = False, ncol = 2)

    def plot_time_series(self):

        """Plot the time series"""

        df = self.get_delta_CO2_storage(as_df=True)
        strip_vars_list = [var.split('_')[1] for var in df.columns]
        fig, ax = plt.subplots(1, 1, figsize = (12, 8))
        fig.patch.set_facecolor('white')
        colour_idx = np.linspace(0, 1, len(df.columns))
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.set_xlabel('$Date$', fontsize = 18)
        ax.set_ylabel('$S_c\/(\mu mol\/CO_2\/m^{-2}\/s^{-1})$', fontsize = 18)
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        plt.plot(self.get_summed_delta_CO2_storage(as_df=True),
                 label = 'Total', color = 'grey')
        for i, var in enumerate(df.columns):
            color = plt.cm.cool(colour_idx[i])
            plt.plot(df[var], label = strip_vars_list[i], color = color)
        plt.legend(loc='lower left', frameon = False, ncol = 2)

    def write_to_csv(self, file_name):

        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_total'] = self.get_summed_delta_CO2_storage(as_df=True)
        df.to_csv(file_name, index_label='DateTime')

    def write_to_netcdf(self, file_path, attrs = None):

        df = self.get_delta_CO2_storage(as_df=True)
        df['dCO2s_total'] = self.get_summed_delta_CO2_storage(as_df=True)
        ds = df.to_xarray()
        ds.attrs = {'Site': self.site,
                    'Heights (m)': ', '.join([str(i) for i in
                                              self.get_heights()]),
                    'Layer depths (m)': ', '.join([str(i) for i in
                                                   self.get_layer_depths()])}
        if attrs: ds.attrs.update(attrs)
        ds.Time.encoding = {'units': 'days since 1800-01-01',
                            '_FillValue': None}
        ds.to_netcdf(file_path, format='NETCDF4')

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_dataframe(this_da):

    df = this_da.to_dataframe().unstack()
    df.columns = df.columns.droplevel(0)
    if df.columns.dtype == object:
        return df[this_da[this_da.dims[1]].data]
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_class(site_name):

    return profile(get_func(site_name), site_name)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_func(site_name):

    import Boyagin
    import CumberlandPlains
    import HowardSprings
    import Litchfield
    import Warra
    import Whroo
    import WombatStateForest as WSF

    funcs_dict = {'Boyagin': Boyagin,
                  'CumberlandPlains': CumberlandPlains,
                  'HowardSprings': HowardSprings,
                  'Litchfield': Litchfield,
                  'Whroo': Whroo,
                  'Warra': Warra,
                  'WombatStateForest': WSF}

    raw_data_read_path = get_configs()['raw_data_read_paths'][site_name]
    return funcs_dict[site_name].get_data(raw_data_read_path)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_configs():

    """Create a configuration file that defines requisite read and write paths
       (reads from same dir as executing script - i.e. this one!)"""

    path = os.path.join(os.path.dirname(__file__), 'paths.ini')
    config = configparser.ConfigParser()
    config.read(path)
    return config
#------------------------------------------------------------------------------
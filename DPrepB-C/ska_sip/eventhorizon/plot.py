#!/usr/bin/env python

"""plot.py: A script for plotting data."""

import sys

import numpy as np

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot as plt
from matplotlib import pylab

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


pylab.rcParams['figure.figsize'] = (4.0, 4.0)
pylab.rcParams['image.cmap'] = 'rainbow'


def uv_cov(vis):
    """Plot the uv-coverage of the data.
        
    Args:
    vis (obj): ARL visibility data.
    """
    try:
        plt.clf()
        plt.plot(vis.data['uvw'][:, 0], vis.data['uvw'][:, 1], '.',
                 color='b')
        plt.plot(-vis.data['uvw'][:, 0], -vis.data['uvw'][:, 1], '.',
                 color='r')
        plt.xlabel('U (wavelengths)')
        plt.ylabel('V (wavelengths)')
        plt.show()
        #
        plt.clf()
        plt.plot(vis.data['uvw'][:, 0], vis.data['uvw'][:, 2], '.',
                 color='b')
        plt.xlabel('U (wavelengths)')
        plt.ylabel('W (wavelengths)')
        plt.show()
        #
        plt.clf()
        plt.plot(vis.data['time'][vis.u > 0.0], vis.data['uvw'][:, 2][vis.u > 0.0],
                 '.', color='b')
        plt.plot(vis.data['time'][vis.u <= 0.0], vis.data['uvw'][:, 2][vis.u <= 0.0],
                 '.', color='r')
        plt.xlabel('U (wavelengths)')
        plt.ylabel('W (wavelengths)')
        plt.show()
        #
        plt.clf()
        plt.hist(vis.w, 50, normed=1, facecolor='green', alpha=0.75)
        plt.xlabel('W (wavelengths)')
        plt.ylabel('Count')
        plt.show()
        plt.clf()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


def uv_dist(vis):
    """Plot the amplitude of the uv-data as a function of uv-distance.
        
    Args:
    vis (obj): ARL visibility data.
    """
    try:
        dist = np.sqrt(vis.data['uvw'][:, 0]**2+vis.data['uvw'][:, 1]**2)
        plt.clf()
        plt.plot(dist, np.abs(vis.data['vis']), '.')
        plt.xlabel('uvdist')
        plt.ylabel('Amp Visibility')
        plt.show()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


def plt_rmsf(phi, rmsf):
    """Plot the rotation measure spread function (RMSF) of the data.
    
    Args:
    phi (numpy array): array of Faraday depths.
    rmsf (numpy array): array of complex point spread function values in Faraday space.
    """
    try:
        plt.clf()
        plt.plot(phi, np.real(rmsf), color='r')
        plt.plot(phi, np.imag(rmsf), color='b')
        plt.plot(phi, np.abs(rmsf), color='k')
        plt.xlabel('Faraday Depth')
        plt.ylabel('Amplitude')
        plt.show()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

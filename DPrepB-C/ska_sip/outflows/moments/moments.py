#!/usr/bin/env python

"""moments.py: A set of functions for handling Faraday Moments."""

import os
import sys

import numpy as np

from processing_components.image.operations import import_image_from_fits, export_image_to_fits

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def load_moments_data(results_dir):
    """Load the full-Stokes images into memory.
    
    Args:
    results_dir (str): directory to save results.
    
    Returns:
    image_temp: ARL image data.
    weights: array of weights per channel (1/sigma**2).
    """
    try:
        if os.path.isdir(results_dir):
            try:
                # Load the channel 0 data as an image template:
                image_temp = import_image_from_fits('%s/imaging_clean_WStack-%s.fits'
                                                    % (results_dir, 0))
                # Fill image_temp with the multi-frequency data:
                image_temp.data = np.concatenate(([import_image_from_fits(
                    '%s/imaging_clean_WStack-%s.fits'
                    % (results_dir, channel)).data
                                                   for channel in range(0, 40)]))
                # Calculate the weights, in the form [channel, stokes, npix, npix]:
                # Initially using std for weights, should consider more robust options.
                weights = np.array([1.0/(np.std(image_temp.data[channel, 0, :, :])**2)
                                    for channel in range(0, 40)])
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
    except:
        print("Input directory does not exist:", sys.exc_info()[0])
        raise
    return image_temp, weights


def weighted_std(values, axis=0, weights=None):
    """Return the weighted average and standard deviation.
 
    Args:
    values (numpy array): numpy array of values.
    axis (int): axis along which to average.
    weights (numpy array): array of weights per channel (1/sigma**2).
    
    Returns:
    weighted standard deviation: the weighted standard deviation
    """
    average = np.average(values, axis=axis, weights=weights)
    variance = np.average((values-average)**2, axis=axis, weights=weights)
    return np.sqrt(variance)  # don't care about the normalisation


def calc_moments(imagecube, weights):
    """Calculate the moments:
        
    Args:
    imagecube (numpy array): full-Stokes image cube.
    weights (numpy array): array of weights per channel (1/sigma**2).
    
    Returns:
    mean_q: Faraday moments image of the mean of Q.
    mean_u: Faraday moments image of the mean of U.
    mean_p: Faraday moments image of the mean of P.
    sigma_q: Faraday moments image of the std. dev. of Q.
    sigma_u: Faraday moments image of the std. dev. of U.
    sigma_p: Faraday moments image of the std. dev. of P.
    """
    stokes_q = imagecube.data[:, 1, :, :]
    stokes_u = imagecube.data[:, 2, :, :]
    stokes_p = np.sqrt(imagecube.data[:, 1, :, :]**2 + imagecube.data[:, 2, :, :]**2)

    mean_q = np.average(stokes_q, axis=0, weights=weights)
    mean_u = np.average(stokes_u, axis=0, weights=weights)
    mean_p = np.average(stokes_p, axis=0, weights=weights)
    sigma_q = weighted_std(stokes_q, axis=0, weights=weights)
    sigma_u = weighted_std(stokes_u, axis=0, weights=weights)
    sigma_p = weighted_std(stokes_p, axis=0, weights=weights)
    return mean_q, mean_u, mean_p, sigma_q, sigma_u, sigma_p


def moments_save_to_disk(moments_im, stokes_type='q', results_dir='./results_dir', outname='mean'):
    """Save the Faraday moments images to disk.
    
    Args:
    rmsynth (numpy array): array of complex numbers from RM Synthesis.
    cellsize (float): advised cellsize in Faraday space.
    maxrm_est (float): maximum observable RM (50 percent sensitivity).
    rmtype (str): the component of the complex numbers to process and save.
    results_dir (str): directory to save results.
    outname (str): outname for saved file.
    
    Returns:
    None
    """
    # Read in the first channel image, and appropriate it as the new moments image:
    im_moments = import_image_from_fits('%s/imaging_clean_WStack-%s.fits' % (results_dir, 0))
    # Place the data into the open image:
    im_moments.data = moments_im
    try:
        if stokes_type == 'p':
            stokes_val = 0.0
        elif stokes_type == 'q':
            stokes_val = 2.0
        elif stokes_type == 'u':
            stokes_val = 3.0
    except:
        print("Unknown value for stokes_type:", sys.exc_info()[0])
        raise

    # This line also adjusts the listed Stokes parameter (use 0.0=? for P):
    im_moments.wcs.wcs.crval = [im_moments.wcs.wcs.crval[0], im_moments.wcs.wcs.crval[1],
                                stokes_val, im_moments.wcs.wcs.crval[3]]
    # Output the file to disk:
    export_image_to_fits(im_moments, '%s/%s_%s.fits' % (results_dir, outname, stokes_type))
    return

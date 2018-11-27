#!/usr/bin/env python

"""rmsynth.py: A set of functions for performing RM Synthesis."""

import os
import sys

import numpy as np

from processing_components.image.operations import import_image_from_fits, export_image_to_fits

from astropy.constants import c

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def rmsynth_advice(frequency, weights):
    """Advise on the RM Synthesis parameters for fully-sampled image datacubes.
        
    Args:
    frequency (numpy array): array of observed frequencies in Hz.
    weights (numpy array): array of weights per channel (1/sigma**2).
    
    Returns:
    lambdasq: array of lambda-squared values.
    lambda0: weighted-average lambda-squared.
    rmsf_est: estimated FWHM of RMSF.
    scale_est: estimated maximum scale in Faraday space.
    maxrm_est: maximum observable RM (50 percent sensitivity).
    cellsize: advised cellsize in Faraday space.
    npixels: advised number of pixels across the range of Faraday depths.
    phi: array of Faraday depths.
    """
    lambdasq = (c.value/frequency)**2
    # lambda0 is a weighted average:
    lambda0 = np.average(lambdasq, weights=weights)
    chanwidth = np.abs(np.mean(np.diff(lambdasq)))
    distwidth = np.max(lambdasq)-np.min(lambdasq)
    minlambdasq = np.min(lambdasq)
    rmsf_est = 2*np.sqrt(3)/distwidth
    scale_est = np.pi/minlambdasq
    maxrm_est = np.sqrt(3)/chanwidth
    print("Recom. RMSF/scale/maxRM:", rmsf_est, "/", scale_est, "/", maxrm_est)
    # Must be highly oversampled to provide RM precision (8 samp./rmsf min.):
    cellsize = rmsf_est/8.0
    # If even, then add a pixel, as has to be symmetric about RM=0.0:
    npixels = round((maxrm_est/cellsize)*2.0)  # *2.0, for + and - sign.
    if npixels % 2 == 0:
        npixels += 1
    else:
        pass
    # Calculate the Faraday-space axis:
    phi = np.linspace(-maxrm_est, maxrm_est, npixels, endpoint=True)
    return lambdasq, lambda0, rmsf_est, scale_est, maxrm_est, cellsize, npixels, phi


def do_rmsynth(weights, phi, complex_p, lambdasq, lambda0):
    """Perform the RM Synthesis.
    
    Args:
    weights (numpy array): array of weights per channel (1/sigma**2).
    phi (numpy array): array of Faraday depths.
    complex_p (numpy array): complex polarisation data (Q+iU).
    lambdasq (numpy array): array of lambda-squared values.
    lambda0 (float): weighted-average lambda-squared.
    
    Returns:
    rmsynth: array of complex numbers from RM Synthesis.
    rmsf: array of complex point spread function values in Faraday space.
    """
    # Calculate size of image:
    ra_len = complex_p.shape[1]
    dec_len = complex_p.shape[2]
    # Calculate the RMSF:
    weighted_sum = np.sum(weights)**-1
    rmsf = (weighted_sum)*np.sum(np.multiply(weights, np.exp(np.outer(phi,
                                 -2.0*1j*(lambdasq-lambda0)))), axis=1)
    # Perform the RM Synthesis:
    rmsynth = np.array([[(weighted_sum)*np.sum(np.multiply(
                        complex_p[:, x, y], np.exp(np.outer(phi,
                        -2.0*1j*(lambdasq-lambda0)))), axis=1)
                        for x in range(ra_len)]
                        for y in range(dec_len)])
    return rmsynth, rmsf, ra_len, dec_len


def rmcube_save_to_disk(rmsynth, cellsize, maxrm_est, rmtype='abs', results_dir='./results_dir', outname='dirty'):
    """Save the RM cubes to disk.
        
    Args:
    rmsynth (numpy array): array of complex numbers from RM Synthesis.
    cellsize (float): advised cellsize in Faraday space.
    maxrm_est (float): maximum observable RM (50 percent sensitivity).
    rmtype (str): the component of the complex numbers to process and save.
    results_dir (str): directory to save results.
    outname (str): outname for saved file.
    """
    # Read in the first channel image, and appropriate it as the new RM cube:
    im_rmsynth = import_image_from_fits('%s/imaging_clean_WStack-%s.fits' % (results_dir, 0))
    # Output the polarised data:
    try:
        if rmtype == 'abs':
            im_rmsynth.data = np.abs(rmsynth)
            stokes_val = 0.0
        elif rmtype == 'real':
            im_rmsynth.data = np.real(rmsynth)
            stokes_val = 2.0
        elif rmtype == 'imag':
            im_rmsynth.data = np.imag(rmsynth)
            stokes_val = 3.0
    except:
        print("Unknown value for rmtype:", sys.exc_info()[0])
        raise
    # Adjust the various axes of the cube:
    im_rmsynth.wcs.wcs.ctype = [im_rmsynth.wcs.wcs.ctype[0], im_rmsynth.wcs.wcs.ctype[1],
                                'FARDEPTH', im_rmsynth.wcs.wcs.ctype[2]]
    im_rmsynth.wcs.wcs.cdelt = [im_rmsynth.wcs.wcs.cdelt[0], im_rmsynth.wcs.wcs.cdelt[1],
                                cellsize, im_rmsynth.wcs.wcs.cdelt[2]]
    im_rmsynth.wcs.wcs.crpix = [im_rmsynth.wcs.wcs.crpix[0], im_rmsynth.wcs.wcs.crpix[1],
                                1.0, im_rmsynth.wcs.wcs.crpix[2]]
    im_rmsynth.wcs.wcs.cunit = [im_rmsynth.wcs.wcs.cunit[0], im_rmsynth.wcs.wcs.cunit[1],
                                'rad / m^2', im_rmsynth.wcs.wcs.cunit[2]]
    # This line also adjusts the listed Stokes parameter (use 0.0=? for P):
    im_rmsynth.wcs.wcs.crval = [im_rmsynth.wcs.wcs.crval[0], im_rmsynth.wcs.wcs.crval[1],
                                -maxrm_est, stokes_val]
    # Tweak the axes into a more sensible order:
    im_rmsynth.data = np.rollaxis(im_rmsynth.data, 2, 0)
    im_rmsynth.data = np.rollaxis(im_rmsynth.data, 2, 1)
    # Output the file to disk:
    export_image_to_fits(im_rmsynth, '%s/rmsynth-%s-%s.fits' % (results_dir, rmtype, outname))
    return


def load_im_data(results_dir):
    """Load the full-Stokes images into memory.
    
    Args:
    results_dir (str): directory to save results.
    
    Returns:
    image_temp: ARL image data.
    frequency: array of observed frequencies in Hz.
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
                # Read the array of the channel frequencies:
                frequency = image_temp.frequency
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
    return image_temp, frequency, weights

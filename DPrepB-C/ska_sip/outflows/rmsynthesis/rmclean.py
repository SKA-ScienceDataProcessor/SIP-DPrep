#!/usr/bin/env python

"""rmclean.py: A set of functions for performing RM-clean."""

import numpy as np

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def rmclean_prep(rmsynth, maxrm_est, npixels, weights, lambdasq, lambda0):
    """Advise on the RM Synthesis parameters for fully-sampled image datacubes.
    
    Args:
    rmsynth (numpy array): the dirty RM cube.
    maxrm_est (float): maximum observable RM (50 percent sensitivity).
    npixels (int): advised number of pixels across the range of Faraday depths.
    weights (numpy array): array of weights per channel (1/sigma**2).
    lambdasq (numpy array): array of lambda-squared values.
    lambda0 (float): weighted-average lambda-squared.
    
    Returns:
    rmsf_double: double sized array of complex point spread function values 
    in Faraday space.
    clean_threshold: estimated noise threshold for cleaning.
    """
    # Calculate the Faraday-space axis for RM-clean:
    phi_double = np.linspace(-maxrm_est*2.0, maxrm_est*2.0, npixels*2.0, \
                             endpoint=True)
    # Calculate the RMSF for RM-clean:
    weighted_sum = np.sum(weights)**-1
    rmsf_double = (weighted_sum)*np.sum(np.multiply(weights, \
                                        np.exp(np.outer(phi_double, \
                                 -2.0*1j*(lambdasq-lambda0)))), axis=1)
    # Calculate the Q/U cleaning thresholds:
    q_threshold = np.std(np.real(rmsynth))
    u_threshold = np.std(np.imag(rmsynth))
    clean_threshold = (q_threshold + u_threshold)/2.0
    return rmsf_double, clean_threshold


def cross_correlate(rmsynth_pixel, rmsf):
    """Define the complex cross-correlation.
   
    Args:
    rmsynth_pixel (numpy array): the dirty RM data for a specific pixel.
    rmsf (numpy array): array of complex point spread function values 
    in Faraday space.
   
    Returns:
    np.fft.ifft(np.fft.fft(rmsynth_pixel)*np.fft.fft(rmsf[::-1])): the 
    function for complex cross-correlation.
    """
    return np.fft.ifft(np.fft.fft(rmsynth_pixel)*np.fft.fft(rmsf[::-1]))


def correlate_signal(rmsynth_pixel, rmsf):
    """Perform complex cross-correlation between the dirty data and the RMSF.
        
    Args:
    rmsynth_pixel (numpy array): the dirty RM data for a specific pixel.
    rmsf (numpy array): array of complex point spread function values 
    in Faraday space.
    
    Returns:
    np.argmax(np.abs(shift)): index of signal with maximum cross-correlation.
    """
    # Cross-correlate the RMSF with the RM Synthesis data in a specific pixel:
    xcorr = cross_correlate(rmsynth_pixel, rmsf)
    # Shift the correlated signal:
    shift = np.fft.fftshift(xcorr)
    # Return the index with the highest amplitude:
    return np.argmax(np.abs(shift))


def form_clean_components(rmsynth_pixel, faraday_peak, rmclean_gain):
    """Extract a complex-valued clean component.
    
    Args:
    rmsynth_pixel (numpy array): the dirty RM data for a specific pixel.
    faraday_peak (int): the index of the peak of the clean component.
    rmclean_gain (float): loop gain for cleaning.
    
    Returns:
    ccomp: the complex-valued clean component.
    """
    # Extract ccomp, as loop gain sized component of complex-valued maxima:
    ccomp = rmclean_gain*rmsynth_pixel[faraday_peak]
    # Provide a de-rotated component, if one so desired it in future:
    # ccomp_derot = cc*np.exp(-2*1j*phi[faradaypeak]*lambda0)
    return ccomp


def shift_scale_rmsf(rmsf_double, phi, cellsize, ccomp, faraday_peak):
    """Shift and scale the RMSF, to the parameters of the found clean component.
        
    Args:
    rmsf_double (numpy array): double sized array of complex point spread 
    function values in Faraday space.
    phi (numpy array): array of Faraday depths.
    cellsize (float): advised cellsize in Faraday space.
    ccomp (float): the complex-valued clean component.
    faraday_peak (int): the index of the peak of the clean component.
    
    Returns:
    ccomp*rmsf_shifted: the shifted and scaled RMSF.
    """
    # Calculate the integer number of pixels required to shift the RMSF:
    faraday_shift = phi[faraday_peak]/cellsize
    faraday_shift = faraday_shift.astype(int)
    # Shift the RMSF and pad with zeros based upon its sign:
    if faraday_shift > 0:
        rmsf_shifted = np.roll(rmsf_double, faraday_shift)
        rmsf_shifted[0:faraday_shift] = 0.0
    elif faraday_shift < 0:
        rmsf_shifted = np.roll(rmsf_double, faraday_shift)
        rmsf_shifted[len(rmsf_shifted)+faraday_shift:len(rmsf_shifted)] = 0.0
    elif faraday_shift == 0:
        rmsf_shifted = np.copy(rmsf_double)
    # The shifted RMSF is double the width of the sampled Faraday space
    # to ensure the shifted beam is subtracted correctly.
    # Truncate the RMSF so it has same dimension as sampled parameter space:
    rmsf_len = len(rmsf_shifted)
    rmsf_shifted = np.delete(rmsf_shifted, np.arange((3*((rmsf_len-1)//4))+1,
                                                     rmsf_len))
    rmsf_shifted = np.delete(rmsf_shifted, np.arange(0, ((rmsf_len-1)//4)))
    # Scale the RMSF by the magnitude of the clean component:
    return ccomp*rmsf_shifted


def rmclean_loop(rmsynth_pixel, rmsf, rmsf_double, phi, rmclean_gain, niter, \
                 clean_threshold, cellsize, rmsf_std):
    """Perform an RM cleaning loop for a specific pixel in the dirty cube.
    
    Args:
    rmsynth_pixel (numpy array): the dirty RM data for a specific pixel.
    rmsf (numpy array): array of complex point spread function values 
    in Faraday space.
    rmsf_double (numpy array): double sized array of complex point spread 
    function values in Faraday space.
    phi (numpy array): array of Faraday depths.
    rmclean_gain (float): loop gain for cleaning.
    niter (int): number of iterations for cleaning.
    clean_threshold (float): estimated noise threshold for cleaning.
    cellsize (float): advised cellsize in Faraday space.
    rmsf_std (float): estimated standard deviation of RMSF.
    
    Returns:
    rmsynth_pixel: the clean RM data for a specific pixel.
    """
    # Empty list of clean components:
    cclist = []
    ccpeak = []
    # Loop over niter iterations:
    for i in range(niter):
        # If the max in Q/U spectra is less than 3sigma, then stop cleaning:
        maxq = np.amax(np.abs(np.real(rmsynth_pixel)))
        maxu = np.amax(np.abs(np.imag(rmsynth_pixel)))
        if (maxq < 3.0*clean_threshold) and (maxu < 3.0*clean_threshold):
            break
        else:
            # Cross-correlate the signal:
            faraday_peak = correlate_signal(rmsynth_pixel, rmsf)
            # Identify the clean component:
            ccomp = form_clean_components(rmsynth_pixel, faraday_peak,
                                        rmclean_gain)
            # Shift and scale the rmsf:
            rmsf_shifted = shift_scale_rmsf(rmsf_double, phi, cellsize,
                                          ccomp, faraday_peak)
            # Subtract the dirty beam from the data:
            rmsynth_pixel = rmsynth_pixel-rmsf_shifted
            # Save the clean component:
            cclist.append(ccomp)
            ccpeak.append(faraday_peak)
    cclist = np.array(cclist)
    ccpeak = np.array(ccpeak)
    # Restore all of the clean components, convolved with the clean beam:
    restored = np.array([[cclist[i]*np.exp(((-((phi[z] -
                          phi[ccpeak[i]])**2))/rmsf_std))
                          for z in range(len(phi))]
                          for i in range(len(ccpeak))])
    # Sum all of the restored clean components and add into the residuals:
    rmsynth_pixel += np.sum(restored, axis=0)
    return rmsynth_pixel


def do_rmclean(rmsynth, phi, rmsf, rmsf_double, rmsf_est, clean_threshold, \
              ra_len, dec_len, cellsize):
    """Perform the RM-clean.
    
    Args:
    rmsynth (numpy array): the dirty RM cube.
    phi (numpy array): array of Faraday depths.
    rmsf (numpy array): array of complex point spread function values 
    in Faraday space.
    rmsf_double (numpy array): double sized array of complex point spread 
    function values in Faraday space.
    rmsf_est (float): estimated FWHM of RMSF.
    clean_threshold (float): estimated noise threshold for cleaning.
    lambda0 (float): weighted-average lambda-squared.
    ra_len (int): size of image in pixels across right ascension.
    dec_len (int): size of image in pixels across declination.
    cellsize (float): advised cellsize in Faraday space.
    
    Returns:
    rmclean: the clean RM cube.
    """
    # Define RM-clean parameters:
    print("Will convolve to FWHM:", str(rmsf_est), "rads/m^2.")
    print("RM-clean noise:", str(clean_threshold), "Jy/bm./rmsf.")
    rmclean_gain = 0.1
    niter = 100
    rmsf_std = 2.0*((rmsf_est/2.355)**2)
    # Perform RM-clean:
    rmclean = np.array([[rmclean_loop(rmsynth[x, y], rmsf, rmsf_double, phi,
                                      rmclean_gain, niter, clean_threshold,
                                      cellsize, rmsf_std)
                         for x in reversed(range(ra_len))]
                         for y in reversed(range(dec_len))])
    return rmclean

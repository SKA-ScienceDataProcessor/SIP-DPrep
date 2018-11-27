#!/usr/bin/env python

"""deconvolution.py: A script for deconvolution of polarisation data."""

import numpy as np

import logging
log = logging.getLogger(__name__)

from data_models.memory_data_models import Image
from data_models.parameters import get_parameter
from libs.image.operations import create_image_from_array
from libs.image.cleaners import hogbom, overlapIndices
from data_models.polarisation import PolarisationFrame

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def hogbom_complex(dirty_q, dirty_u, psf_q, psf_u, window, gain, thresh, niter, fracthresh):
    """Clean the point spread function from a dirty Q+iU image
    
    This uses the complex Hogbom CLEAN for polarised data (2016MNRAS.462.3483P)
    
    The starting-point for the code was the standard Hogbom clean algorithm available in ARL.
    
    Args:
    dirty_q (numpy array): The dirty Q Image, i.e., the Q Image to be deconvolved.
    dirty_u (numpy array): The dirty U Image, i.e., the U Image to be deconvolved.
    psf_q (numpy array): The point spread-function in Stokes Q.
    psf_u (numpy array): The point spread-function in Stokes U.
    window (float): Regions where clean components are allowed. If True, entire dirty Image is allowed.
    gain (float): The "loop gain", i.e., the fraction of the brightest pixel that is removed in each iteration.
    thresh (float): Cleaning stops when the maximum of the absolute deviation of the residual is less than this value.
    niter (int): Maximum number of components to make if the threshold `thresh` is not hit.
    fracthresh (float): The predefined fractional threshold at which to stop cleaning.
    
    Returns:
    comps.real: real clean component image.
    comps.imag: imaginary clean component image.
    res.real: real residual image.
    res.imag: imaginary residual image.
    """
    
    assert 0.0 < gain < 2.0
    assert niter > 0
    
    # Form complex Q+iU from the polarisation data:
    dirty_complex = 1j*dirty_u
    dirty_complex += dirty_q
    
    log.info("hogbom_mod: Max abs in dirty image = %.6f" % np.max(np.abs(dirty_complex)))
    absolutethresh = max(thresh, fracthresh * np.absolute(dirty_complex).max())
    log.info("hogbom_mod: Start of minor cycle")
    log.info("hogbom_mod: This minor cycle will stop at %d iterations or peak < %s" % (niter, absolutethresh))
    
    comps = np.zeros(dirty_complex.shape, dtype='complex128')
    res = np.array(dirty_complex)
    
    assert np.all(psf_q == psf_u)
    
    pmax = psf_q.max()
    assert pmax > 0.0
    log.info("hogbom: Max abs in dirty Image = %.6f" % np.absolute(res).max())
    for i in range(niter):
        if window is not None:
            mx, my = np.unravel_index((np.absolute(res * window)).argmax(), dirty_complex.shape)
        else:
            mx, my = np.unravel_index((np.absolute(res)).argmax(), dirty_complex.shape)
        mval = res[mx, my] * gain / pmax
        comps[mx, my] += mval
        a1o, a2o = overlapIndices(dirty_complex, psf_q, mx, my)
        if niter < 10 or i % (niter // 10) == 0:
            log.info("hogbom: Minor cycle %d, peak %s at [%d, %d]" % (i, res[mx, my], mx, my))
        res[a1o[0]:a1o[1], a1o[2]:a1o[3]] -= psf_q[a2o[0]:a2o[1], a2o[2]:a2o[3]] * mval
        if np.abs(res[mx, my]) < absolutethresh:
            log.info("hogbom: Stopped at iteration %d, peak %s at [%d, %d]" % (i, res[mx, my], mx, my))
            break
    log.info("hogbom: End of minor cycle")
    return comps.real, comps.imag, res.real, res.imag


def deconvolve_cube_complex(dirty: Image, psf: Image, **kwargs) -> (Image, Image):
    """ Clean using the complex Hogbom algorithm for polarised data (2016MNRAS.462.3483P)
        
    The algorithm available is:
    hogbom-complex: See: Pratley L. & Johnston-Hollitt M., (2016), MNRAS, 462, 3483.
    
    This code is based upon the deconvolve_cube code for standard Hogbom clean available in ARL.
    
    Args:
    dirty (numpy array): The dirty image, i.e., the image to be deconvolved.
    psf (numpy array): The point spread-function.
    window (float): Regions where clean components are allowed. If True, entire dirty Image is allowed.
    algorithm (str): Cleaning algorithm: 'hogbom-complex' only.
    gain (float): The "loop gain", i.e., the fraction of the brightest pixel that is removed in each iteration.
    threshold (float): Cleaning stops when the maximum of the absolute deviation of the residual is less than this value.
    niter (int): Maximum number of components to make if the threshold `thresh` is not hit.
    fractional_threshold (float): The predefined fractional threshold at which to stop cleaning.

    Returns:
    comp_image: clean component image.
    residual_image: residual image.
    """
    assert isinstance(dirty, Image), "Type is %s" % (type(dirty))
    assert isinstance(psf, Image), "Type is %s" % (type(psf))
    
    window_shape = get_parameter(kwargs, 'window_shape', None)
    if window_shape == 'quarter':
        qx = dirty.shape[3] // 4
        qy = dirty.shape[2] // 4
        window = np.zeros_like(dirty.data)
        window[..., (qy + 1):3 * qy, (qx + 1):3 * qx] = 1.0
        log.info('deconvolve_cube_complex: Cleaning inner quarter of each sky plane')
    else:
        window = None
    
    psf_support = get_parameter(kwargs, 'psf_support', None)
    if isinstance(psf_support, int):
        if (psf_support < psf.shape[2] // 2) and ((psf_support < psf.shape[3] // 2)):
            centre = [psf.shape[2] // 2, psf.shape[3] // 2]
            psf.data = psf.data[..., (centre[0] - psf_support):(centre[0] + psf_support),
                                (centre[1] - psf_support):(centre[1] + psf_support)]
            log.info('deconvolve_cube_complex: PSF support = +/- %d pixels' % (psf_support))

    algorithm = get_parameter(kwargs, 'algorithm', 'msclean')
    
    if algorithm == 'hogbom-complex':
        log.info("deconvolve_cube_complex: Hogbom-complex clean of each polarisation and channel separately")
        gain = get_parameter(kwargs, 'gain', 0.7)
        assert 0.0 < gain < 2.0, "Loop gain must be between 0 and 2"
        thresh = get_parameter(kwargs, 'threshold', 0.0)
        assert thresh >= 0.0
        niter = get_parameter(kwargs, 'niter', 100)
        assert niter > 0
        fracthresh = get_parameter(kwargs, 'fractional_threshold', 0.1)
        assert 0.0 <= fracthresh < 1.0
        
        comp_array = np.zeros(dirty.data.shape)
        residual_array = np.zeros(dirty.data.shape)
        for channel in range(dirty.data.shape[0]):
            for pol in range(dirty.data.shape[1]):
                if pol == 0 or pol == 3:
                    if psf.data[channel, pol, :, :].max():
                        log.info("deconvolve_cube_complex: Processing pol %d, channel %d" % (pol, channel))
                        if window is None:
                            comp_array[channel, pol, :, :], residual_array[channel, pol, :, :] = \
                                hogbom(dirty.data[channel, pol, :, :], psf.data[channel, pol, :, :],
                                       None, gain, thresh, niter, fracthresh)
                        else:
                            comp_array[channel, pol, :, :], residual_array[channel, pol, :, :] = \
                                hogbom(dirty.data[channel, pol, :, :], psf.data[channel, pol, :, :],
                                       window[channel, pol, :, :], gain, thresh, niter, fracthresh)
                    else:
                        log.info("deconvolve_cube_complex: Skipping pol %d, channel %d" % (pol, channel))
                if pol == 1:
                    if psf.data[channel, 1:2, :, :].max():
                        log.info("deconvolve_cube_complex: Processing pol 1 and 2, channel %d" % (channel))
                        if window is None:
                            comp_array[channel, 1, :, :], comp_array[channel, 2, :, :], residual_array[channel, 1, :, :], residual_array[channel, 2, :, :] = hogbom_complex(dirty.data[channel, 1, :, :], dirty.data[channel, 2, :, :], psf.data[channel, 1, :, :], psf.data[channel, 2, :, :], None, gain, thresh, niter, fracthresh)
                        else:
                            comp_array[channel, 1, :, :], comp_array[channel, 2, :, :], residual_array[channel, 1, :, :], residual_array[channel, 2, :, :] = hogbom_complex(dirty.data[channel, 1, :, :], dirty.data[channel, 2, :, :], psf.data[channel, 1, :, :], psf.data[channel, 2, :, :], window[channel, pol, :, :], gain, thresh, niter, fracthresh)
                    else:
                        log.info("deconvolve_cube_complex: Skipping pol 1 and 2, channel %d" % (channel))
                if pol == 2:
                    continue
                
        comp_image = create_image_from_array(comp_array, dirty.wcs, polarisation_frame=PolarisationFrame('stokesIQUV'))
        residual_image = create_image_from_array(residual_array, dirty.wcs, polarisation_frame=PolarisationFrame('stokesIQUV'))

    else:
        raise ValueError('deconvolve_cube_complex: Unknown algorithm %s' % algorithm)
    
    return comp_image, residual_image

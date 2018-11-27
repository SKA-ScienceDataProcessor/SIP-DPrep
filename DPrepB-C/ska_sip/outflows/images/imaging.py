#!/usr/bin/env python

"""imaging.py: The script for imaging."""

import sys

import numpy as np

from data_models.polarisation import PolarisationFrame
from processing_components.imaging.base import create_image_from_visibility, advise_wide_field
from processing_components.image.operations import export_image_to_fits
from processing_components.imaging.weighting import weight_visibility
from workflows.serial.imaging.imaging_serial import invert_serial

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def wstack(vis, npixel_advice, cell_advice, channel, results_dir):
    """Do w-stacked imaging of visibility data.
        
    Args:
    vis (obj): ARL visibility data.
    npixel_advice (float): number of pixels in output image.
    cell_advice (float): cellsize in output image.
    channel (int): channel number to be imaged (affects output filename).
    results_dir (str): directory to save results.
    
    Returns:
    dirty: dirty image.
    psf: image of psf.
    """
    try:
        vis_slices = len(np.unique(vis.time))
        print("There are %d timeslices" % vis_slices)
        # Obtain advice on w-proj parameters:
        advice = advise_wide_field(vis)
        # Create a model image:
        model = create_image_from_visibility(vis, cellsize=cell_advice, npixel=npixel_advice, phasecentre=vis.phasecentre, polarisation_frame=PolarisationFrame('stokesIQUV'))
        # Weight the visibilities:
        vis, _, _ = weight_visibility(vis, model)
        
        # Create a dirty image:
        dirty, sumwt = invert_serial(vis, model, context='wstack', facets=1, vis_slices=42)
        # Create the psf:
        psf, sumwt = invert_serial(vis, model, dopsf=True, context='wstack', facets=1, vis_slices=42)
        
        # Save to disk:
        export_image_to_fits(dirty, '%s/imaging_dirty_WStack-%s.fits'
                             % (results_dir, channel))
        export_image_to_fits(psf, '%s/imaging_psf_WStack-%s.fits'
                             % (results_dir, channel))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return dirty, psf


def image_2d(vis, npixel_advice, cell_advice, channel, results_dir):
    """Do 2D imaging of visibility data.
        
    Args:
    vis (obj): ARL visibility data.
    npixel_advice (float): number of pixels in output image.
    cell_advice (float): cellsize in output image.
    channel (int): channel number to be imaged (affects output filename).
    results_dir (str): directory to save results.
    
    Returns:
    dirty: dirty image.
    psf: image of psf.
    """
    try:
        vis_slices = len(np.unique(vis.time))
        print("There are %d timeslices" % vis_slices)
        # Obtain advice on w-proj parameters:
        advice = advise_wide_field(vis)
        # Create a model image:
        model = create_image_from_visibility(vis, cellsize=cell_advice, npixel=npixel_advice, phasecentre=vis.phasecentre, polarisation_frame=PolarisationFrame('stokesIQUV'))
        # Weight the visibilities:
        vis, _, _ = weight_visibility(vis, model)
        
        # Create a dirty image:
        dirty, sumwt = invert_serial(vis, model, context='2d', vis_slices=1, padding=2)
        # Create the psf:
        psf, sumwt = invert_serial(vis, model, dopsf=True, context='2d', vis_slices=1, padding=2)
        
        # Save to disk:
        export_image_to_fits(dirty, '%s/imaging_dirty_WStack-%s.fits'
                             % (results_dir, channel))
        export_image_to_fits(psf, '%s/imaging_psf_WStack-%s.fits'
                             % (results_dir, channel))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return dirty, psf

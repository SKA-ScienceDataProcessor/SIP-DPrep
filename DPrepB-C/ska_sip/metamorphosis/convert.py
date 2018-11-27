#!/usr/bin/env python

"""convert.py: A script to converting data in a measurement set."""

from data_models.polarisation import convert_linear_to_stokes, convert_circular_to_stokes, PolarisationFrame

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def convert_to_stokes(vis, poldef):
    """Convert the polarisation frame data into Stokes parameters.
        
    Args:
    vis (obj): ARL visibility data.
    poldef (str): definition of the polarisation frame.
    
    Returns:
    vis: Converted visibility data.
    """
    if poldef == 'lin':
        vis = convertlineartostokes(vis)
    if poldef == 'circ':
        vis = convertcirculartostokes(vis)
    return vis


def convertlineartostokes(vis):
    """Convert linear polarisations (XX, XY, YX, YY) into Stokes parameters.
    
    Args:
    vis (obj): ARL visibility data.
    
    Returns:
    vis: Converted visibility data.
    """
    vis.data['vis'] = convert_linear_to_stokes(vis.data['vis'], polaxis=1)
    vis.polarisation_frame = PolarisationFrame('stokesIQUV')
    return vis


def convertcirculartostokes(vis):
    """Convert circular polarisations (RR, RL, LR, LL) into Stokes parameters.
        
    Args:
    vis (obj): ARL visibility data.
    
    Returns:
    vis: Converted visibility data.
    """
    vis.data['vis'] = convert_circular_to_stokes(vis.data['vis'], polaxis=1)
    vis.polarisation_frame = PolarisationFrame('stokesIQUV')
    return vis

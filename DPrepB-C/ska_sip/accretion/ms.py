#!/usr/bin/env python

"""ms.py: A script for loading measurement set data."""

import sys

import numpy

import time as t

from astropy import constants as constants
from astropy import units as u
from astropy.coordinates import SkyCoord

from processing_components.visibility.base import create_visibility_from_ms
from data_models.memory_data_models import Visibility, BlockVisibility, Configuration
from data_models.polarisation import PolarisationFrame, ReceptorFrame, correlate_polarisation

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def load(msfilename='', channumselect=0, poldef='lin'):
    """Load a measurement set and define the polarisation frame.
        
        Args:
        msfilename (str): file name of the measurement set.
        channumselect (int): channel number to load.
        poldef (str): definition of the polarisation frame.
        
        Returns:
        vis: The visibility data.
    """
    try:
        # Load in a single MS:
        vis = create_visibility_from_ms(msfilename, channumselect)[0]
        # Set the polarisation frame:
        if poldef == 'lin':
            vis.polarisation_frame = PolarisationFrame('linear')
        if poldef == 'circ':
            vis.polarisation_frame = PolarisationFrame('circular')
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return vis


def load_maps(msfilename='', poldef='lin'):
    """Load a measurement set and define the polarisation frame.
        
        Args:
        msfilename (str): file name of the measurement set.
        poldef (str): definition of the polarisation frame.
        
        Returns:
        vis: The visibility data.
    """
    try:
        # Load in a single MS:
        vis = create_visibility_from_ms_maps(msfilename, poldef)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return vis


def create_visibility_from_ms_maps(msname, poldef):
    """ Minimal MS to Visibility converter
        
        The MS format is much more general than the ARL Visibility so we cut many corners. This requires casacore to be
        installed. If not an exception ModuleNotFoundError is raised.
        
        Creates a list of Visibilities, one per phasecentre
    """
    
    try:
        from casacore.tables import table  # pylint: disable=import-error
    except ModuleNotFoundError:
        raise ModuleNotFoundError("casacore is not installed")
    
    tab = table(msname)
    print(tab.info())
    fields = numpy.unique(tab.getcol('FIELD_ID'))
    print("Found unique field ids %s" % fields)

    # for field in fields:
    # First get the main table information
    ms = tab.query("FIELD_ID==%d" % fields[0])
    print("Found %d rows for field %d" % (ms.nrows(), fields[0]))
    time = ms.getcol('TIME')
    weight = ms.getcol('WEIGHT')
    uvw = -1 * ms.getcol('UVW')
    antenna1 = ms.getcol('ANTENNA1')
    antenna2 = ms.getcol('ANTENNA2')
    integration_time = ms.getcol('INTERVAL')
    ddid = ms.getcol('DATA_DESC_ID')

    # Get polarisation info
    # poltab = table('%s/POLARIZATION' % msname, ack=False)
    # corr_type = poltab.getcol('CORR_TYPE')
    # TODO: Do interpretation correctly
    # polarisation_frame = PolarisationFrame('stokesIQUV')
    # Set the polarisation frame:
    if poldef == 'lin':
        polarisation_frame = PolarisationFrame('linear')
    if poldef == 'circ':
        polarisation_frame = PolarisationFrame('circular')

    # Get configuration
    anttab = table('%s/ANTENNA' % msname, ack=False)
    mount = anttab.getcol('MOUNT')
    names = anttab.getcol('NAME')
    diameter = anttab.getcol('DISH_DIAMETER')
    xyz = anttab.getcol('POSITION')
    configuration = Configuration(name='', data=None, location=None, names=names, xyz=xyz, mount=mount, frame=None, receptor_frame=ReceptorFrame("linear"), diameter=diameter)
    # Get phasecentres
    fieldtab = table('%s/FIELD' % msname, ack=False)
    pc = fieldtab.getcol('PHASE_DIR')[fields[0], 0, :]
    print(pc[0], pc[1])
    phasecentre = SkyCoord(ra=[pc[0]] * u.rad, dec=pc[1] * u.rad, frame='icrs', equinox='J2000')
    
    channels = len(numpy.transpose(ms.getcol('DATA'))[0])
    print("Found %d channels" % (channels))
    
    spwtab = table('%s/SPECTRAL_WINDOW' % msname, ack=False)
    cfrequency = spwtab.getcol('CHAN_FREQ')
    
    cchannel_bandwidth = spwtab.getcol('CHAN_WIDTH')
    channel_bandwidth = numpy.array([cchannel_bandwidth[dd] for dd in ddid])[:, 0]

    # Now get info from the subtables
    maps_data = list()
    for channum in range(channels):
        try:
            vis = ms.getcol('DATA')[:, channum, :]
        except IndexError:
            raise IndexError("channel number exceeds max. within ms")
    
        frequency = numpy.array([cfrequency[dd] for dd in ddid])[:, channum]
        uvw *= frequency[:, numpy.newaxis] / constants.c.to('m/s').value
        
        vis_list = Visibility(uvw=uvw, time=time, antenna1=antenna1, antenna2=antenna2,
                              frequency=frequency, vis=vis,
                              weight=weight, imaging_weight=weight,
                              integration_time=integration_time,
                              channel_bandwidth=channel_bandwidth,
                              configuration=configuration,
                              phasecentre=phasecentre,
                              polarisation_frame=polarisation_frame)

        maps_data.append(vis_list)

    return maps_data

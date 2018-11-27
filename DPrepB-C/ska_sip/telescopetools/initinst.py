#!/usr/bin/env python

"""initinst.py: A script to initialise parameters for the instrument."""

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def init_inst(inst):
    """Initialise any parameters that are specific to a named telescope.
    Can expand to easy initialisation of other parameters as needed.
        
    Args:
    inst (str): name of telescope.
    
    Returns:
    poldef: polarisation frame of observed data.
    """
    if inst == 'LOFAR':
        poldef = 'lin'
    if inst == 'JVLA':
        poldef = 'circ'
    return poldef

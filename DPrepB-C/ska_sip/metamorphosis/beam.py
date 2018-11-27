#!/usr/bin/env python

""" beam.py: A script to load in the LOFAR station response.
"""

import casacore.tables as pt
import numpy as np

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def beam_me_up(inputs_dir, ms1):
    """Load the LOFAR station responses.
        
    Args:
    inputs_dir (str): location of input directory.
    ms1 (str): name of measurement set.
    
    Returns:
    beams: array of station responses.
    """
    
    #`msname`
    #Name of the Measurement Set.
    #`inverse`
    #Compute the inverse of the LOFAR beam (default False).
    #`useElementResponse`
    #Include the effect of the dual dipole (element) beam (default True).
    #`useArrayFactor`
    #Include the effect of the station and tile array factor (default
    #True).
    #`useChanFreq`
    #Compute the phase shift for the station beamformer using the channel
    #frequency instead of the subband reference frequency. This option
    #should be enabled for Measurement Sets that contain multiple subbands
    #compressed to single channels inside a single spectral window
    #(default: False).
    import lofar.stationresponse as st
    beams = st.stationresponse(msname='%s/%s' % (inputs_dir, ms1), inverse=False, useElementResponse=True, useArrayFactor=True, useChanFreq=False)

    return beams


def apply_station_beams(vis, beams, channel):
    #
    # This is a dummy function. It currently does nothing. It will eventually apply the LOFAR
    # stations beams to the data.
    # Correct for the Station Beam. This mostly compensates for the element beam effects and the projection of the dipoles on the sky. However, the LOFAR fields are big, and the projection of the dipoles vary across the field of view.
    """
    # times are MJD in seconds:
    times = vis.data['time']

    # Find the indices of unique times within the MS:
    _, indices = np.unique(times, return_index=True)

    beams.setDirection(0.01,0.5*np.pi)
    beams.evaluateStation(time=times[0],station=0)

    for i in range( len(indices) ):
        #print("times", len(indices), indices[i], indices[i+1], i, times[indices[i]])
    
        #START = t.time()
        response = beams.evaluate(time=times[indices[i]])
        # :,channel_number = all stations, single channel.
        response = response[:,channel]
        # Can refer to a specific station via, response[station_number]
        # response[28] would now contain the beam response for station 28 at the previously selected time, as a 2x2 Jones matrix.
        #END = t.time()
    
        first_index = indices[i]
        if i+1 < len(indices):
            second_index = indices[i+1]
        elif i+1 == len(indices):
            second_index = len(vis.data)
        else:
            print("ERROR: unknown error has occurred.")

        temp_len = second_index-first_index
        # Get it in the form J=[[j_xx, j_xy],[j_yx, j_yy]]
        visi_data = np.reshape(vis.data['vis'][first_index:second_index],(temp_len,2,2))
        response_1 = response[vis.data['antenna1'][first_index:second_index]]
        response_2 = response[vis.data['antenna2'][first_index:second_index]]
    
        # Need the inverse of the first beam:
        beam_1 = np.linalg.inv(response_1)
        # Need the Hermitian of the inverse of the second beam:
        beam_2 = np.transpose(np.conj(np.linalg.inv(response_2)), axes=(0,2,1))
        # Now calculate: corrected = beam_i^-1 uncorrected herm(beam_j^-1)
        #vis.data['vis'][first_index:second_index] = np.reshape(beam_1*visi_data*beam_2,(temp_len,4))
        for q in range(len(visi_data)):
            vis.data['vis'][q:q+1] = np.reshape( np.dot(np.dot(beam_1[q],visi_data[q]),beam_2[q]),(1,4))
    """
    return vis

#!/usr/bin/env python

""" iono.py: A script to apply the ionospheric Faraday rotation corrections.
    """

import numpy as np

from astropy.constants import c

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"


def get_ion_rotation_measures(vis, inputs_dir, ms1):
    """Calculate the ionospheric Faraday rotation for each time slice.
    
    Args:
    vis (ARL object): the visibility data.
    inputs_dir (str): location of input directory.
    ms1 (str): name of measurement set.
    
    Returns:
    ionRM: the ionospheric rotation measures.
    times: the times corresponding to each RM.
    indices: the indices corresponding to unique time periods.
    """
    
    from RMextract.getRM import getRM
    import RMextract.PosTools as PosTools

    import pyrap.tables as tab
    
    # The Measurement Set defines the station and dipole positions, the phase centre, and the channel frequencies (and reference frequency) for which the LOFAR beam will be evaluated.
    myms = tab.table(inputs_dir + '/' + ms1)
    stations = tab.table(myms.getkeyword('ANTENNA')).getcol('NAME')
    lofar_stat_pos = tab.table(myms.getkeyword('ANTENNA')).getcol('POSITION')  # in ITRF metres
    # Times are MJD in seconds:
    times = vis.data['time']
    # Find the indices of unique times within the MS:
    _, indices = np.unique(times, return_index=True)
    # Calculate the correction for each time indices:
    ionRM = []
    for i in range(len(indices)):
        use_azel = False  # use az/el or ra/dec?
        use_mean = True  # report for the mean of the station positions?
        time_offset = 120.0  # offset time at start and end to ensure all needed values are calculated,
        time_difference = np.median(np.diff(times[indices]))/2.0
        min_time = times[indices][i] - time_difference
        max_time = times[indices][i] + time_difference
        time_step = max_time-min_time  # frequency to obtain solutions in seconds.
        # Get the results:
        result = getRM(use_azel=use_azel, use_mean=use_mean, timerange=[min_time, max_time], timestep=time_step, stat_names=stations, stat_positions=lofar_stat_pos, useEMM=True, TIME_OFFSET=time_offset)
        RM = result['RM']
        ionRM.append(np.median(RM[stations[0]]))
    ionRM = np.array(ionRM)
    return ionRM, times, indices


def get_ion_rotation_measures_maps(vis, stations, lofar_stat_pos):
    """Calculate the ionospheric Faraday rotation for each time slice.
        
    Args:
    vis (ARL object): the visibility data.
    stations (list): list of stations.
    lofar_stat_pos (list): list of station positions.
    
    Returns:
    ionRM: the ionospheric rotation measures.
    times: the times corresponding to each RM.
    indices: the indices corresponding to unique time periods.
    """
    from RMextract.getRM import getRM
    import RMextract.PosTools as PosTools
    # Times are MJD in seconds:
    times = vis.data['time']
    # Find the indices of unique times within the MS:
    _, indices = np.unique(times, return_index=True)
    # Calculate the correction for each time indices:
    ionRM = []
    for i in range(len(indices)):
        use_azel = False  # use az/el or ra/dec?
        use_mean = True  # report for the mean of the station positions?
        time_offset = 120.0  # offset time at start and end to ensure all needed values are calculated,
        time_difference = np.median(np.diff(times[indices]))/2.0
        min_time = times[indices][i] - time_difference
        max_time = times[indices][i] + time_difference
        time_step = max_time-min_time  # frequency to obtain solutions in seconds.
        # Get the results:
        result = getRM(use_azel=use_azel, use_mean=use_mean, timerange=[min_time, max_time], timestep=time_step, stat_names=stations, stat_positions=lofar_stat_pos, useEMM=True, TIME_OFFSET=time_offset)
        RM = result['RM']
        ionRM.append(np.median(RM[stations[0]]))
    ionRM = np.array(ionRM)
    return ionRM, times, indices


def correct_ion_faraday(vis, ionRM, times, indices):
    """Calculate the ionospheric Faraday rotation for each time slice.
            
    Args:
    vis (ARL object): the visibility data.
    ionRM (numpy array): the ionospheric rotation measures.
    times (numpy array): the times corresponding to each RM.
    indices (numpy array): the indices corresponding to unique time periods.
            
    Returns:
    vis: the visibility data.
    """
    # Times are MJD in seconds:
    times_new = vis.data['time']
    # Find the indices of unique times within the MS:
    _, indices_new = np.unique(times_new, return_index=True)
    if np.all(times[indices] == times_new[indices_new]):
        pass  # no data is flagged
    elif np.any(times[indices] != times_new[indices_new]):
        # If this ever happened, the script could be modified to recalculate the ionospheric RMs, just for this channel:
        print("ERROR: ionospheric Faraday rotation will not be correctly applied.")
    # Calculate the correction for each time indices:
    for i in range(len(indices_new)):
        # Calculate the needed quantities from the RM:
        faraday_rotation = ionRM[i]
        lambda_meas = c.value/vis.frequency[0]
        rad_rotation = faraday_rotation*(lambda_meas**2)
        # Calculate the response (for linear feeds only):
        response = np.broadcast_to(np.array([[np.cos(rad_rotation), np.sin(rad_rotation)], [-np.sin(rad_rotation), np.cos(rad_rotation)]]), (len(vis.data), 2, 2))
        # Process the indices:
        first_index = indices_new[i]
        if i+1 < len(indices_new):
            second_index = indices_new[i+1]
        elif i+1 == len(indices_new):
            second_index = len(vis.data)
        else:
            print("ERROR: unknown error has occurred.")
        temp_len = second_index-first_index
        # Get it in the form J=[[j_xx, j_xy],[j_yx, j_yy]]:
        visi_data = np.reshape(vis.data['vis'][first_index:second_index], (temp_len, 2, 2))
        response_1 = response[vis.data['antenna1'][first_index:second_index]]
        response_2 = response[vis.data['antenna2'][first_index:second_index]]
        # Need the inverse of the first beam:
        beam_1 = np.linalg.inv(response_1)
        # Need the Hermitian of the inverse of the second beam:
        beam_2 = np.transpose(np.conj(np.linalg.inv(response_2)), axes=(0, 2, 1))
        # Now calculate: corrected = beam_i^-1 uncorrected herm(beam_j^-1)
        for q in range(len(visi_data)):
            vis.data['vis'][q:q+1] = np.reshape(np.dot(np.dot(beam_1[q], visi_data[q]), beam_2[q]), (1, 4))
    return vis

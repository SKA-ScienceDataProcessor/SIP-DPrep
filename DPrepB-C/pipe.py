#!/usr/bin/env python

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"

import os
import time as t
import subprocess
import argparse

import numpy as np

from processing_components.image.operations import import_image_from_fits, export_image_to_fits, \
    qa_image
from processing_components.visibility.operations import append_visibility

from ska_sip.uvoperations.filter import uv_cut, uv_advice
from ska_sip.telescopetools.initinst import init_inst
from ska_sip.loaddata.ms import load
from ska_sip.pipelines.dprepb import dprepb_imaging, arl_data_future

import dask
import dask.array as da
from dask.distributed import Client
from distributed.diagnostics import progress
from distributed import wait

from confluent_kafka import Producer
import pickle


def main(ARGS):
    """
    Initialising launch sequence.
    """
    # ------------------------------------------------------
    # Print some stuff to show that the code is running:
    print("")
    os.system("printf 'A demonstration of a \033[5mDPrepB/DPrepC\033[m SDP pipeline\n'")
    print("")
    # Set the directory for the moment images:
    MOMENTS_DIR = ARGS.outputs + '/MOMENTS'
    # Check that the output directories exist, if not then create:
    os.makedirs(ARGS.outputs, exist_ok=True)
    os.makedirs(MOMENTS_DIR, exist_ok=True)
    # Set the polarisation definition of the instrument:
    POLDEF = init_inst(ARGS.inst)
    
    # Setup Variables for SIP services
    # ------------------------------------------------------
    # Define the Queue Producer settings:
    if ARGS.queues:
        queue_settings = {'bootstrap.servers': 'scheduler:9092', 'message.max.bytes': 100000000}
    
    # Setup the Confluent Kafka Queue
    # ------------------------------------------------------
    if ARGS.queues:
        # Create an SDP queue:
        sip_queue = Producer(queue_settings)
    
    # Define a Data Array Format
    # ------------------------------------------------------
    def gen_data(channel):
        return np.array([vis1[channel], vis2[channel], channel, None, None, False, False, \
                         ARGS.plots, float(ARGS.uvcut), float(ARGS.pixels), POLDEF, ARGS.outputs, \
                         float(ARGS.angres), None, None, None, None, None, None, ARGS.twod, \
                         npixel_advice, cell_advice])
    
    # Setup the Dask Cluster
    # ------------------------------------------------------
    starttime = t.time()

    dask.config.set(get=dask.distributed.Client.get)
    client = Client(ARGS.daskaddress)  # scheduler for Docker container, localhost for P3.
    
    print("Dask Client details:")
    print(client)
    print("")

    # Define channel range for 1 subband, each containing 40 channels:
    channel_range = np.array(range(int(ARGS.channels)))

    # Load the data into memory:
    print("Loading data:")
    print("")
    vis1 = [load('%s/%s' % (ARGS.inputs, ARGS.ms1), range(channel, channel+1), POLDEF) \
            for channel in range(0, int(ARGS.channels))]
    vis2 = [load('%s/%s' % (ARGS.inputs, ARGS.ms2), range(channel, channel+1), POLDEF) \
            for channel in range(0, int(ARGS.channels))]

    # Prepare Measurement Set
    # ------------------------------------------------------
    # Combine MSSS snapshots:
    vis_advice = append_visibility(vis1[0], vis2[0])
    
    # Apply a uv-distance cut to the data:
    vis_advice = uv_cut(vis_advice, float(ARGS.uvcut))
    npixel_advice, cell_advice = uv_advice(vis_advice, float(ARGS.uvcut), float(ARGS.pixels))
    
    # Begin imaging via the Dask cluster
    # ------------------------------------------------------
    # Submit data for each channel to the client, and return an image:

    # Scatter all the data in advance to all the workers:
    print("Scatter data to workers:")
    print("")
    big_job = [client.scatter(gen_data(channel)) for channel in channel_range]

    # Submit jobs to the cluster and create a list of futures:
    futures = [client.submit(dprepb_imaging, big_job[channel], pure=False, retries=3) \
               for channel in channel_range]

    print("Imaging on workers:")
    # Watch progress:
    progress(futures)

    # Wait until all futures are complete:
    wait(futures)
    
    # Check that no futures have errors, if so resubmit:
    for future in futures:
        if future.status == 'error':
            print("ERROR: Future", future, "has 'error' status, as:")
            print(client.recreate_error_locally(future))
            print("Rerunning...")
            print("")
            index = futures.index(future)
            futures[index].cancel()
            futures[index] = client.submit(dprepb_imaging, big_job[index], pure=False, retries=3)

    # Wait until all futures are complete:
    wait(futures)

    # Gather results from the futures:
    results = client.gather(futures, errors='raise')

    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        print("Adding QA to queue:")
        for result in results:
            sip_queue.produce('qa', pickle.dumps(qa_image(result), protocol=2))

        sip_queue.flush()

    # Return the data element of each ARL object, as a Dask future:
    futures = [client.submit(arl_data_future, result, pure=False, retries=3) for result in results]

    progress(futures)

    wait(futures)


    # Calculate the Moment images
    # ------------------------------------------------------
    # Now use 'distributed Dask arrays' in order to parallelise the Moment image calculation:
    # Construct a small Dask array for every future:
    print("")
    print("Calculating Moment images:")
    print("")
    arrays = [da.from_delayed(future, dtype=np.dtype('float64'), shape=(1, 4, 512, 512)) \
              for future in futures]

    # Stack all small Dask arrays into one:
    stack = da.stack(arrays, axis=0)

    # Combine chunks to reduce overhead - is initially (40, 1, 4, 512, 512):
    stack = stack.rechunk((1, 1, 4, 64, 64))

    # Spread the data around on the cluster:
    stack = client.persist(stack)
    # Data is now coordinated by the single logical Dask array, 'stack'.

    # Save the Moment images:
    print("Compute Moment images and save to disk:")
    print("")
    # First generate a template:
    image_template = import_image_from_fits('%s/imaging_dirty_WStack-%s.fits' % (ARGS.outputs, 0))

    # Output mean images:
    # I:
    image_template.data = stack[:, :, 0, :, :].mean(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Mean-%s.fits' % (MOMENTS_DIR, 'I'))

    # Q:
    image_template.data = stack[:, :, 1, :, :].mean(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Mean-%s.fits' % (MOMENTS_DIR, 'Q'))

    # U:
    image_template.data = stack[:, :, 2, :, :].mean(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Mean-%s.fits' % (MOMENTS_DIR, 'U'))

    # P:
    image_template.data = da.sqrt((da.square(stack[:, :, 1, :, :]) + \
                                   da.square(stack[:, :, 2, :, :]))).mean(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Mean-%s.fits' % (MOMENTS_DIR, 'P'))

    # Output standard deviation images:
    # I:
    image_template.data = stack[:, :, 0, :, :].std(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Std-%s.fits' % (MOMENTS_DIR, 'I'))

    # Q:
    image_template.data = stack[:, :, 1, :, :].std(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Std-%s.fits' % (MOMENTS_DIR, 'Q'))

    # U:
    image_template.data = stack[:, :, 2, :, :].std(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Std-%s.fits' % (MOMENTS_DIR, 'U'))

    # P:
    image_template.data = da.sqrt((da.square(stack[:, :, 1, :, :]) + \
                                   da.square(stack[:, :, 2, :, :]))).std(axis=0).compute()
    # Run QA on ARL objects and produce to queue:
    if ARGS.queues:
        sip_queue.produce('qa', pickle.dumps(qa_image(image_template), protocol=2))
    # Export the data to disk:
    export_image_to_fits(image_template, '%s/Std-%s.fits' % (MOMENTS_DIR, 'P'))

    # Flush queue:
    if ARGS.queues:
        sip_queue.flush()

    # Make a tarball of moment images:
    subprocess.call(['tar', '-cvf', '%s/moment.tar' % (MOMENTS_DIR), '%s/' % (MOMENTS_DIR)])
    subprocess.call(['gzip', '-9f', '%s/moment.tar' % (MOMENTS_DIR)])

    endtime = t.time()
    print(endtime-starttime)


# Define the arguments for the pipeline:
AP = argparse.ArgumentParser()
AP.add_argument('-d', '--daskaddress', help='Address of the Dask scheduler [default scheduler:8786]', default='scheduler:8786')
AP.add_argument('-c', '--channels', help='Number of channels to process [default 40]', default=40)
AP.add_argument('-inp', '--inputs', help='Input data directory [default /data/inputs]', default='/data/inputs')
AP.add_argument('-out', '--outputs', help='Output data directory [default /data/outputs]', default='/data/outputs')
AP.add_argument('-ms1', '--ms1', help='Measurement Set 1 [default sim-1.ms]', default='sim-1.ms')
AP.add_argument('-ms2', '--ms2', help='Measurement Set 2 [default sim-2.ms]', default='sim-2.ms')
AP.add_argument('-q', '--queues', help='Enable Queues? [default False]', default=True)
AP.add_argument('-p', '--plots', help='Output diagnostic plots? [default False]', default=False)
AP.add_argument('-2d', '--twod', help='2D imaging [True] or wstack imaging [False]? [default False]', default=False)
AP.add_argument('-uv', '--uvcut', help='Cut-off for the uv-data [default 450]', default=450.0)
AP.add_argument('-a', '--angres', help='Force the angular resolution to be consistent across the band, in arcmin FWHM [default 8.0]', default=8.0)
AP.add_argument('-pix', '--pixels', help='The number of pixels/sampling across the observing beam [default 5.0]', default=5.0)
AP.add_argument('-ins', '--inst', help='Instrument name (for future use) [default LOFAR]', default='LOFAR')
ARGS = AP.parse_args()
main(ARGS)

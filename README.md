SDP Integration Prototype (SIP) DPrepB/C Pipeline
========================================

This is a project to create a pipeline for the Square Kilometre Array (SKA) Science Data Processor (SDP) Integration Prototype (SIP), in order to process spectropolarimetric I, Q, U, V data. This is classified within SDP as a "DPrepB/DPrepC" or spectral imaging pipeline. Further details on the code are provided in SKA SDP Memo 081.

This parallelised code provides a fully-Pythonic pipeline for processing spectropolarimetric radio interferometry visibility data into final data products. The code uses the ARL.

Primary uses includes full-Stokes imaging and Faraday Moments. Beam correction is not yet implemented.

The pipeline is designed and intended as a demonstrator of [`Dask`](https://dask.pydata.org/en/latest/) as one plausible Execution Framework, and uses both 'Dask distributed' and 'distributed Dask arrays' in order to process the data. Monitoring and logging can be accessed via Bokeh at <http://localhost:8787>. The pipeline outputs .fits data products, which is one feasible option that is usable by a typical radio astronomer.

The pipeline can be deployed to a local Docker installation. No IP address is required for the scheduler, but can be manually specified as an argument to the Python code if desired. The running of this pipeline within a Docker swarm cluster on P3-AlaSKA, alongside integration with other SIP services, are currently under consideration and active development. Services such as Queues and Quality Assessment (QA) are now fully implemented in this released version - a Dockerised QA aggregator container is built during installation, with the consumed QA messages being readable via `docker logs ska-sip-dprepb-c-pipeline_qa_1`.

Various additional features will be implemented and released in due course, including the source-finding and RM Synthesis codes that constitute parts of a LOFAR MSSS/MAPS pipeline. Many of the functions that are used for this purpose are already included in the code available here.

The aim is to provide brief and user-friendly documentation: if any details are missing, overly verbose, or unclear, then please get in contact so that the documentation can be updated.


## Thanks
This pipeline has been developed with huge thanks to the contributions from: Ben Mort, Fred Dulwich, Tim Cornwell, and many more throughout the SIP team.


## Quick-start
Assuming that Docker is already installed, it is possible to download the pipeline and data, to build and deploy the Docker containers, and to image 10 channels of the simulated data, using just 5 lines of code:
```bash
git clone https://github.com/jamiefarnes/SKA-SIP-DPrepB-C-Pipeline
cd SKA-SIP-DPrepB-C-Pipeline
docker-compose up -d --build
docker exec -it ska-sip-dprepb-c-pipeline_scheduler_1 bash
python SKA-SIP-DPrepB-C-Pipeline/DPrepB-C/pipe.py -c=10
```

The consumed QA messages are readable from the QA aggregator via:
```
docker logs ska-sip-dprepb-c-pipeline_qa_1
```

## Running the Pipeline
Based on: <https://github.com/dask/dask-docker>

Note that no adjustment of any files or parameters should be required. The `docker-compose.yml` file should work 'as is'. The docker-compose file will bind volumes so that they are accessible from within the Docker container. The `source` for each volume should not need to be modified, and will automatically point to the location of the simulated data in `inputs-docker`.

The cluster can then be deployed to a local Docker installation with [`docker-compose`](https://docs.docker.com/compose/overview/).

To start the cluster:

```bash
docker-compose up -d --build
```
Note that the --build argument is not required if the cluster has already been built.

To destroy the cluster:

```bash
docker-compose rm -s -f
```

The cluster creates four services:

-   **scheduler**: The Dask scheduler. Published on <http://localhost:8787>. This service can be accessed for monitoring and logging using Bokeh.

-   **worker**: The Dask worker service.

-   **notebook**: Published on <http://localhost:8888> but must log in with the
token printed in the logs when starting this container
(eg. `docker logs ska-sip-dprepb-c-pipeline_notebook_1`)

-   **qa**: The QA service, on which a QA aggregator is automatically started.

The cluster can then be accessed using:
```bash
docker exec -it ska-sip-dprepb-c-pipeline_scheduler_1 bash
```

The Docker container has been tested, and will automatically connect to the scheduler.

Finally, the DPrepB/C pipeline can be run using:
```bash
python SKA-SIP-DPrepB-C-Pipeline/DPrepB-C/pipe.py [--help]
```
The default settings should work together with the simulated datasets. If your machine is struggling to process all 40 channels with Dask given finite resources, then one can use the ```--channels``` argument:
```bash
python SKA-SIP-DPrepB-C-Pipeline/DPrepB-C/pipe.py -c=10
```

The pipeline performs w-stacking by default. In order to use a 2D imaging process:
```bash
python SKA-SIP-DPrepB-C-Pipeline/DPrepB-C/pipe.py -c=10 -2d=True
```

## Simulated Data
Two simulated datasets are included in measurement set format for the purpose of testing the pipeline. These datasets include 40 spectral channels. The simulation is of a field of view which contains four radio sources - each with different polarised radio properties. These data were simulated using [`OSKAR`](https://github.com/OxfordSKA/OSKAR).


## Dependencies

See Dockerfiles.

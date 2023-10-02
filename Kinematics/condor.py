"""
This is a helper file. This file sets the proxy for condor jobs and allocates resources.
"""

import logging
import os
from coffea import processor

def move_X509():
    try:
        _x509_localpath = (
            [
                line
                for line in os.popen("voms-proxy-info").read().split("\n")
                if line.startswith("path")
            ][0]
            .split(":")[-1]
            .strip()
        )
    except Exception as err:
        raise RuntimeError(
            "x509 proxy could not be parsed, try creating it with 'voms-proxy-init'"
        ) from err
    _x509_path = f'/scratch/{os.environ["USER"]}/{_x509_localpath.split("/")[-1]}'
    os.system(f"cp {_x509_localpath} {_x509_path}")
    return os.path.basename(_x509_localpath)

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )

def runCondor(cores=1, memory="2 GB", disk="1 GB", death_timeout = '60', workers=4):
    from distributed import Client
    from dask_jobqueue import HTCondorCluster

    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"
    _x509_path = move_X509()

    cluster = HTCondorCluster(
        cores=1,
        memory="2 GB",
        disk="1 GB",
        death_timeout = '60',
        job_extra_directives={
            "+JobFlavour": '"tomorrow"',
            "log": "dask_job_output.$(PROCESS).$(CLUSTER).log",
            "output": "dask_job_output.$(PROCESS).$(CLUSTER).out",
            "error": "dask_job_output.$(PROCESS).$(CLUSTER).err",
            "should_transfer_files": "yes",
            "when_to_transfer_output": "ON_EXIT_OR_EVICT",
            "+SingularityImage": '"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7:latest"',
            "Requirements": "HasSingularityJobStart",
            "request_GPUs" : "1",
            "InitialDir": f'/scratch/{os.environ["USER"]}',
            "transfer_input_files": f'{_x509_path},{os.environ["EXTERNAL_BIND"]}'
        },
        job_script_prologue=[
            "export XRD_RUNFORKHANDLER=1",
            f"export X509_USER_PROXY={_x509_path}",
        ]
    )
    cluster.adapt(minimum=1, maximum=workers)
    executor = processor.DaskExecutor(client=Client(cluster))
    return executor
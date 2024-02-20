from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
from coffea import processor
from coffea.analysis_tools import PackedSelection
import awkward as ak
import hist
from matplotlib import pyplot as plt
import mplhep as hep
import logging
import numpy as np

# events = NanoEventsFactory.from_root(
#     "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
#     schemaclass=NanoAODSchema.v7,
#     metadata={"Dataset":"MET_Run2018"}
#     ).events()

import os

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

def runCondor(cores=1, memory="2 GB", disk="1 GB", death_timeout = '60', workers=4):
    from distributed import Client
    from dask_jobqueue import HTCondorCluster

    os.environ["CONDOR_CONFIG"] = "/etc/condor/condor_config"
    _x509_path = move_X509()

    cluster = HTCondorCluster(
        cores=1,
        memory="2 GB",
        disk="1 GB",
        job_extra_directives={
            "+JobFlavour": '"longlunch"',
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
    return executor, Client(cluster)

class JetKinem(processor.ProcessorABC):
    def __init__(self):
        pass
    def process(self, events):
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis=1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 ,axis=1))
        events = events[BasicCuts.all("pt_cut","eta_cut")]
        Jets = events.Jet

        JetHist= (
            hist.Hist.new
            .Reg(100,0.,400., label="$p_t$ (GeV)")
            .Double()
            )
        JetHist.fill(ak.flatten(Jets.pt))

        #Prepare the output
        output = {
            "Histograms": {
                "Jetpt" : JetHist
            }
        }
        return output
    def postprocess(self,events):
        pass

executor = "condor"
#executor = "futures"

files = {
        "MET_Run2018A": [
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/10C73E73-0C15-2F4B-9E0B-E3DE1C54A597.root",
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/1E8B7F5A-4B29-8F46-B2E1-549805E5CBB2.root",
        ]
    }
#For futures execution
if executor == "futures" :
    
    #files = getDataset(keymap=inputs.keymap, mode="divide", files=inputs.files)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=4),
        schema=NanoAODSchema,
        chunksize= 500000 ,
        maxchunks= None,
        xrootdtimeout=120
    )
    Output = futures_run(
        files,
        "Events",
        processor_instance=JetKinem()
    )

elif executor == "condor" :
    #Create a console log in case of a warning 
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )
    print("Preparing to run at condor...\n")
    executor , client = runCondor()

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=500000,
        maxchunks=None,
        xrootdtimeout=300,
    )
    print("Starting the workers...\n")
    Output = runner(
        files,
        treename="Events",
        processor_instance=JetKinem()
    )

print(Output)

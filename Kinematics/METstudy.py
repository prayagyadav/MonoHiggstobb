"""
This file Studies MET.
Author: Prayag Yadav
Created: 8 Oct 2023
"""

#################################
# Import the necessary packages #
#################################

import awkward as ak
import argparse
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
from coffea import util
import condor
import hist
import json
import logging
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
plt.style.use(hep.style.CMS)
#import uproot

##############################
# Define the terminal inputs #
##############################

parser = argparse.ArgumentParser()
parser.add_argument(
    "Mode",
    help="Enter MC to run Monte Carlo Samples or enter Data to run Data samples"
    )
parser.add_argument(
    "-e",
    "--executor",
    choices=["futures","condor", "dask"],
    help="Enter where to run the file : futures(local) or dask(local) or condor",
    default="futures",
    type=str
)
parser.add_argument(
    "-c",
    "--chunk_size",
    help="Enter the chunksize; by default 100k",
    type=int ,
    default=100000
    )
parser.add_argument(
    "-m",
    "--max_chunks",
    help="Enter the number of chunks to be processed; by default None ie full dataset",
    type=int
    )
parser.add_argument(
    "-w",
    "--workers",
    help="Enter the number of workers to be employed for processing in local; by default 4",
    type=int ,
    default=4 
    )
inputs = parser.parse_args()

########################
# Define the processor #
########################

class METstudy(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        pass
    def process(self, events):
        self.cutflow["Total_Events"] = len(events) #Total Number of events
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut")]
        self.cutflow["ReducedEvents"] = len(events)

        #Apply MET filters
        METflags = PackedSelection()
        METflags.add("goodVertices", events.Flag.goodVertices)
        METflags.add("tightHalo", events.Flag.globalTightHalo2016Filter)
        METflags.add("hbheNoise", events.Flag.HBHENoiseFilter)
        METflags.add("hbheNoiseIso", events.Flag.HBHENoiseIsoFilter)
        METflags.add("eebadSC", events.Flag.eeBadScFilter)
        METflags.add("EcalDeadcell", events.Flag.EcalDeadCellTriggerPrimitiveFilter)
        METflags.add("badPFmuon", events.Flag.BadPFMuonFilter )
        METflags.add("Ecalbadcalib",events.Flag.ecalBadCalibFilter )

        flags = METflags.all(
            "goodVertices",
            "tightHalo",
            "hbheNoise",
            "hbheNoiseIso",
            "eebadSC",
            "EcalDeadcell",
            "badPFmuon",
            "Ecalbadcalib"
            )
        
        MET = events.MET[flags]
        self.cutflow["After_Flags"] = len(MET)

        #Creating the MET pt histogram
        METHist_pt = (
            hist.
            Hist.
            new.
            StrCat(["noflags","flags"], name="type").
            Reg(100,0,500, name="pt", label="$p_t$ (GeV)")
            .Double()
            )
        
        #Note: MET has no eta

        #Creating the MET phi histogram
        METHist_phi = (
            hist.
            Hist.
            new.
            StrCat(["noflags","flags"], name="type").
            Reg(100,-np.pi,np.pi, name="phi", label="$\phi$")
            .Double()
            )
        
        #Fill the histogram
        METHist_pt.fill(type="noflags", pt= events.MET.pt)
        METHist_pt.fill(type="flags", pt=  MET.pt)
        METHist_phi.fill(type="noflag", phi=events.MET.phi)
        METHist_phi.fill(type="flags", phi=MET.phi)

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "METpt": METHist_pt ,
                "METphi": METHist_phi 
            }
        }
        return output
    def postprocess(self, accumulator):
        pass

#################################
# Run the processor #
#################################

#Create a console log in case of a warning 
logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )

Mode = inputs.Mode

#For futures execution
if inputs.executor == "futures" :
    with open("fileset.json") as f: #load the fileset
        files = json.load(f)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = futures_run(
        files[Mode],
        "Events",
        processor_instance=METstudy()
    )

#For dask execution
elif inputs.executor == "dask" :
    print("WARNING: This feature is still in development!\nAttemping to run nevertheless ...")
    from dask.distributed import Client , LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    cluster.scale(inputs.workers)
    client.upload_file("fileset.json")
    with open("fileset.json") as f: #load the fileset
        files = json.load(f)
    dask_run = processor.Runner(
        executor = processor.DaskExecutor(client=client),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = dask_run(
        files[Mode],
        "Events",
        processor_instance=METstudy()
    )

#For condor execution
elif inputs.executor == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("fileset.json")
    with open("fileset.json") as f:
        files = json.load(f)

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=inputs.chunk_size,
        maxchunks=inputs.max_chunks,
        xrootdtimeout=300,
    )
    print("Running...\n")
    Output = runner(
        files[Mode],
        treename="Events",
        processor_instance=METstudy(),
    )

#################################
# Create the output file #
#################################
output_file = f"{Mode}METstudy.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

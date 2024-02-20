"""
Insert description of the file.
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
from monoHbbtools.Utilities import condor
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

class MyProcessor(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        pass
    def process(self, events):
        self.cutflow["Total_Events"] = len(events) #Total Number of events
        #Apply the basic cuts like pt and eta
        # BasicCuts = PackedSelection()
        # BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        # BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        # events = events[BasicCuts.all("pt_cut")]
        # self.cutflow["ReducedEvents"] = len(events)

        #Creating histograms
        x_min = 0
        x_max = 500
        nbins = 100
        h = (
            hist.
            Hist.
            new.
            StrCat(["type1","type2"], name="Attribute").
            Reg(nbins,x_min,x_max, name="Name", label="xlabel")
            .Double()
            )
        
        #Fill the histogram
        h.fill(Attribute="type1", Name = events.MET.pt )

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "Histogram": h ,
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
        files["Data"],
        "Events",
        processor_instance=MyProcessor()
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
        files["Data"],
        "Events",
        processor_instance=MyProcessor()
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
        files["Data"],
        treename="Events",
        processor_instance=MyProcessor(),
    )

#################################
# Create the output file #
#################################
output_file = f"Output.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

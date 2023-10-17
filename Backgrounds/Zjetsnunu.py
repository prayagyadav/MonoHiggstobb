"""
This script studies the Z--> \nu + \nu + jets background .
Author: Prayag Yadav
Created: 11 Oct 2023
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
from monoHbbtools import Load
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
parser.add_argument(
    "-f",
    "--files",
    help="Enter the number of files to be processed",
    type=int 
    )
inputs = parser.parse_args()

########################
# Define the processor #
########################

class Zjetsnunu(processor.ProcessorABC):
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
        events = events[BasicCuts.all("pt_cut", "eta_cut")]
        self.cutflow["ReducedEvents"] = len(events)

        #MET Filters
        flags = PackedSelection()
        flags.add("goodVertices", events.Flag.goodVertices)
        flags.add("tightHalo", events.Flag.globalTightHalo2016Filter)
        flags.add("hbheNoise", events.Flag.HBHENoiseFilter)
        flags.add("hbheNoiseIso", events.Flag.HBHENoiseIsoFilter)
        flags.add("eebadSC", events.Flag.eeBadScFilter)
        flags.add("EcalDeadcell", events.Flag.EcalDeadCellTriggerPrimitiveFilter)
        flags.add("badPFmuon", events.Flag.BadPFMuonFilter )
        flags.add("Ecalbadcalib",events.Flag.ecalBadCalibFilter )

        flagcut = flags.all(
            "goodVertices",
            "tightHalo",
            "hbheNoise",
            "hbheNoiseIso",
            "eebadSC",
            "EcalDeadcell",
            "badPFmuon",
            "Ecalbadcalib"
            )
        
        events = events[flagcut]
        
        Jets = events.Jet
        #Apply the btag 
        btag_WP_medium = 0.3040 # Medium Working Point
        btag_WP_tight = 0.7476 # Tight Working Point
        GoodJetCut = Jets.btagDeepFlavB > btag_WP_tight 
        ak4_BJets_tight = Jets[GoodJetCut]
        self.cutflow["ak4bJetsTight"] = ak.sum(ak.num(ak4_BJets_tight)) #No of ak4 tight bjets

        #Create Dijets
        def ObtainDiJets(jet):
            jet = jet[ak.num(jet)>1]
            Dijet = jet[:,0]+jet[:,1]
            return Dijet 
        DiJets = ObtainDiJets(ak4_BJets_tight)
        self.cutflow["bbDiJets"] = len(DiJets) #No of bb Dijets

        #Creating histograms
        x_min = 0
        x_max = 500
        nbins = 100
        DiJetHist = (
            hist.
            Hist.
            new.
            Reg(nbins,x_min,x_max).
            Double()
            )
        
        #Fill the histogram
        DiJetHist.fill( DiJets.mass )

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "Dijet": DiJetHist ,
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

    fileset = Load.Loadfileset("../monoHbbtools/Load/newfileset.json")
    fileset_dict = fileset.getraw()
    fileset_dict = Load.buildFileset(fileset_dict["Data"]["MET"],"fnal")
    try :
        fileset_dict = {"MET": fileset_dict["Data"]["MET"]["MET_Run2018A"][:inputs.files]}
    except :
        print("Numbers of files requested is greater than the numbers of files in first dictionary of the fileset.")
        raise ValueError
    # with open("../monoHbbtools/Load/newfileset.json") as f: #load the fileset
    #     files = json.load(f)
    # files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = futures_run(
        fileset_dict,
        "Events",
        processor_instance=Zjetsnunu()
    )

#For dask execution
elif inputs.executor == "dask" :
    print("WARNING: This feature is still in development!\nAttemping to run nevertheless ...")
    from dask.distributed import Client , LocalCluster
    cluster = LocalCluster()
    client = Client(cluster)
    cluster.scale(inputs.workers)
    client.upload_file("../monoHbbtools/Load/newfileset.json")
    with open("newfileset.json") as f: #load the fileset
        files = json.load(f)
    files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}
    dask_run = processor.Runner(
        executor = processor.DaskExecutor(client=client),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=120
    )
    Output = dask_run(
        files,
        "Events",
        processor_instance=Zjetsnunu()
    )

#For condor execution
elif inputs.executor == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("../monoHbbtools/Load/newfileset.json")
    with open("../monoHbbtools/Load/newfileset.json") as f:
        files = json.load(f)
    files = {"MET": files["Data"]["MET"]["MET_Run2018A"][:inputs.files]}

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=inputs.chunk_size,
        maxchunks=inputs.max_chunks,
        xrootdtimeout=300,
    )
    print("Running...\n")
    Output = runner(
        files,
        treename="Events",
        processor_instance=Zjetsnunu(),
    )

#################################
# Create the output file #
#################################
output_file = f"Zjetsnunu.coffea"
print("Saving the output to : " , output_file)
util.save(output= Output, filename=output_file)
print(f"File {output_file} saved.")

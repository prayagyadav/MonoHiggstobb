"""
This file obtains Data and MC samples and plots a few kinematics by making some object selections criteria,  filtering and de-noising.
Author: Prayag Yadav
Created: 6 Oct 2023
"""

#################################
# Import the necessary packages #
#################################

import awkward as ak
import argparse
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoAODSchema #,NanoEventsFactory 
from coffea import processor
from coffea.util import save
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
    choices=["local","condor"],
    help="Enter where to run the file : local or condor",
    default="local",
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

class JetKinem(processor.ProcessorABC):
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
        Jets = events.Jet
        self.cutflow["ReducedJets"] = ak.sum(ak.num(Jets)) #No of jets passing the BasicCuts

        #Apply the btag 
        btag_WP_medium = 0.3040 # Medium Weight Parameter
        GoodJetCut = Jets.btagDeepFlavB > btag_WP_medium 
        ak4_BJets_med = Jets[GoodJetCut]
        self.cutflow["ak4bJetsMedium"] = ak.sum(ak.num(ak4_BJets_med)) #No of ak4 medium bjets

        #Create Dijets
        def ObtainDiJets(jet, bjet):
            jet = jet[ak.num(jet)>1]
            bjet = bjet[ak.num(bjet)>1]
            Dijet = jet[:,0]+jet[:,1]
            Dibjet = bjet[:,0]+bjet[:,1]
            return Dijet , Dibjet
        DiJets , DiJets_bb = ObtainDiJets(Jets, ak4_BJets_med)
        self.cutflow["DiJets"] = len(DiJets) #No of Dijets
        self.cutflow["bbDiJets"] = len(DiJets_bb) #No of bb Dijets

        #Create the histograms
        #1. bTag score histogram
        TagHist = hist.Hist.new.Reg(20,0.,1.).Double().fill(ak.flatten(Jets.btagDeepFlavB))

        #2. Jets pt : Untagged and Tagged 
        JetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(50,0.,500., name="pt", label="$p_t$ (GeV)")
            .Double()
            )
        JetHist.fill(Type="Untagged", pt = ak.flatten(Jets.pt))
        JetHist.fill(Type="btagDeepFlavB", pt = ak.flatten(ak4_BJets_med.pt))

        #3. DiJets Mass : Untagged and Tagged
        DiJetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(50,0.,500., name="mass", label="Mass (GeV)")
            .Double()
            )
        DiJetHist.fill(Type="Untagged", mass = DiJets.mass)
        DiJetHist.fill(Type="btagDeepFlavB", mass = DiJets_bb.mass)

        #4. MET histogram
        metHist = (
            hist.Hist.new.Reg(100,0,500).Double()
        )
        metHist.fill(events.MET.pt)

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "Tag" : TagHist ,
                "Jetpt" : JetHist ,
                "DiJetMass" : DiJetHist ,
                "MET": metHist 
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

#For local execution
if inputs.executor == "local" :
    with open("fileset.json") as f: #load the fileset
        files = json.load(f)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=inputs.workers),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
        xrootdtimeout=300
    )
    Output = futures_run(
        files[Mode],
        "Events",
        processor_instance=JetKinem()
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
        processor_instance=JetKinem(),
    )

#################################
# Create the output file #
#################################
output_file = f"Datakinematics.coffea"
print("Saving the output to : " , output_file)
save(output= Output, filename=output_file)
print(f"File {output_file} saved.")


"""
This file obtains Data and MC samples and plots a few kinematics.
Author: Prayag Yadav
Created: 30 Sept 2023
"""

#################################
# Import the necessary packages #
#################################

import awkward as ak
import argparse
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoAODSchema #,NanoAODSchema 
from coffea import processor
import condor
import hist
import json
import logging
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
import os
plt.style.use(hep.style.CMS)
import sys
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
inputs = parser.parse_args()

########################
# Define the processor #
########################

class JetKinem(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow array
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
        self.cutflow["JetCut"] = ak.sum(ak.num(Jets)) #No of jets passing the BasicCuts

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
        TagHist = hist.Hist.new.Reg(20,0,1).Double().fill(ak.flatten(Jets.btagDeepFlavB))

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

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "Tag" : TagHist ,
                "Jetpt" : JetHist ,
                "DiJetMass" : DiJetHist 
            }
        }
        return output
    def postprocess(self, accumulator):
        pass

###################
# Load the fileset #
###################

with open("fileset.json") as f:
    files = json.load(f)

#################################
# Run the processor #
#################################

#Create a log file
logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )

Mode = inputs.Mode

#For local execution
if inputs.executor == "local" :
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(compression=None, workers=4),
        schema=NanoAODSchema,
        chunksize= inputs.chunk_size ,
        maxchunks= inputs.max_chunks,
    )
    Output = futures_run(
        files[Mode],
        "Events",
        processor_instance=JetKinem()
    )

#For condor execution
elif inputs.executor == "condor" :
    #sys.exit("Condor feature not available yet!")
    print("Preparing to run at condor\n")
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

    
#######################
# Plot the histograms #
#######################

#Load the output
print("The flow of events :\n ", Output["Cutflow"])
Tag_Hist = Output["Histograms"]["Tag"]
Jetpt_Hist = Output["Histograms"]["Jetpt"]
DiJetMass_Hist = Output["Histograms"]["DiJetMass"]

#1. bTag score histogram
Tag_Hist.plot()
plt.savefig(f"Tag{Mode}.png", dpi= 320)
plt.clf()

#2. Jets pt : Untagged and Tagged 
x_min = 0.
x_max = 500.
bin_size = 10
n_bins=int((x_max - x_min)/bin_size)
#fig , ax= plt.subplots(figsize=(10,10))
fig , ax= plt.subplots()
hep.histplot(Jetpt_Hist["Untagged",:], 
             #bins=bins ,
             histtype="fill",
             color="b",
             alpha=0.7,
             edgecolor="black",
             label=r"Untagged",
             ax=ax
            )
hep.histplot(Jetpt_Hist["btagDeepFlavB",:], 
             #bins=bins ,
             histtype="fill",
             color="r",
             alpha=0.7,
             edgecolor="black",
             label=r"btagDeepFlavB",
             ax=ax
            )
ax.set_title("Jet $p_t$", y=1.0, pad = -35 , fontsize=25, color="#053B50")
ax.set_xlabel("$p_t$ (GeV)", fontsize=20)
ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
hep.cms.label("Preliminary",data = Mode == "Data", rlabel="unknown $fb^{-1}$")
ax.legend()
fig.savefig(f"Jets{Mode}.png", dpi= 300)
plt.clf()

#3. DiJets Mass : Untagged and Tagged
x_min = 0.
x_max = 500.
bin_size = 10
n_bins=int((x_max - x_min)/bin_size)
fig , ax= plt.subplots()
hep.histplot(DiJetMass_Hist["Untagged",:], 
             #bins=bins ,
             histtype="fill",
             color="b",
             alpha=0.7,
             edgecolor="black",
             label=r"Untagged",
             ax=ax
            )
hep.histplot(DiJetMass_Hist["btagDeepFlavB",:], 
             #bins=bins ,
             histtype="fill",
             color="r",
             alpha=0.7,
             edgecolor="black",
             label=r"btagDeepFlavB",
             ax=ax
            )
ax.set_title("DiJet Invariant Mass", y=1.0, pad = 35 , fontsize=25, color="#053B50" )
ax.set_xlabel("Mass (GeV)", fontsize=20)
ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size*10))
hep.cms.label("Preliminary",data = Mode == "Data", rlabel="unknown $fb^{-1}$")
ax.legend()
fig.savefig(f"DiJets{Mode}.png", dpi= 300)
plt.clf()

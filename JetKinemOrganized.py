"""
This file obtains data and mc samples and plots a few kinematics.
Author: Prayag Yadav
Created: 30 Sept 2023
"""

#################################
# Import the necessary packages #
#################################

#import uproot
import awkward as ak
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents import NanoEventsFactory , NanoAODSchema
from coffea import processor
import hist
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
plt.style.use(hep.style.CMS)

########################
# Define the processor #
########################

class JetKinem(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow array
        self.cutflow = []
        pass
    def process(self, events):
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", events.Jet.pt > 25.0 )
        BasicCuts.add("eta_cut", abs( events.Jet.eta ) < 2.5 )
        Jets = events.Jet[BasicCuts.all("pt_cut")]

        #Apply the btag 
        GoodJetCut = PackedSelection()
        btag_WP_medium = 0.3040 # Medium Weight Parameter
        GoodJetCut.add("btag", Jets.btagDeepFlavB > btag_WP_medium )
        ak4_BJets_med = Jets[GoodJetCut.all("btag")]

        #Create Dijets
        def ObtainDiJets(jet, bjet):
            jet = jet[ak.num(jet)>1]
            bjet = bjet[ak.num(bjet)>1]
            Dijet = jet[:,0]+jet[:,1]
            Dibjet = bjet[:,0]+bjet[:,1]
            return Dijet , Dibjet
        DiJets , DiJets_bb = ObtainDiJets(Jets, ak4_BJets_med)

        #Create the histograms
        #1. bTag score histogram
        TagHist = hist.Hist.new.Reg(20,-1,1).Double().fill(ak.flatten(Jets.btagDeepFlavB))

        #2. Jets pt : Untagged and Tagged 
        JetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(100,0.,400., name="pt", label="$p_t$ (GeV)")
            .Double()
            )
        JetHist.fill(Type="Untagged", pt = ak.flatten(Jets.pt))
        JetHist.fill(Type="btagDeepFlavB", pt = ak.flatten(ak4_BJets_med.pt))

        #3. DiJets Mass : Untagged and Tagged
        DiJetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(100,0.,400., name="mass", label="Mass (GeV)")
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

###################
# Load the events #
###################

redirector = "root://cmsxrootd.fnal.gov//"
events = NanoEventsFactory.from_root(
    redirector+"/store/mc/RunIIAutumn18NanoAODv7/MonoHTobb_ZpBaryonic_TuneCP2_13TeV_madgraph-pythia8/NANOAODSIM/Nano02Apr2020_rp_102X_upgrade2018_realistic_v21-v1/10000/0EE0D641-EDAE-D547-ABAD-56D54B768C5B.root",
    schemaclass= NanoAODSchema.v7,
    metadata= {"Dataset":"Single Electrons"}
).events()

#################################
# Run the processor #
#################################

Output = JetKinem(events)

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
plt.savefig("Tag.png", dpi= 320)
plt.clf()

#2. Jets pt : Untagged and Tagged 
x_min = 0.
x_max = 400.
bin_size = 4
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
hep.cms.label("Preliminary",data=False, rlabel="unknown $fb^{-1}$")
ax.legend()
fig.savefig("Jets.png", dpi= 300)
plt.clf()

#3. DiJets Mass : Untagged and Tagged
x_min = 0.
x_max = 400.
bin_size = 4
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
hep.cms.label("Preliminary",data=False, rlabel="unknown $fb^{-1}$")
ax.legend()
fig.savefig("DiJets.png", dpi= 300)
plt.clf()


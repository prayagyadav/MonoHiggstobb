""" 
~~~~~~~~~~~~~~~~~~~~~~~PROCESSOR SCRIPT~~~~~~~~~~~~~~~~~~~~
This script studies the Z--> \nu + \nu + jets background .

/Author/: Prayag Yadav
/Created/: 11 Oct 2023
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

#################################
# Import the necessary packages #
#################################

import awkward as ak
from coffea.analysis_tools import PackedSelection
from coffea import processor
import hist

########################
# Define the processor #
########################

class Zjetsnunu(processor.ProcessorABC):
    def __init__(self, keylist):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        self.keylist = keylist
        
    def process(self, events):
        dataset = events.metadata["dataset"]
        self.mode = dataset
        cutflow = {}
        cutflow["Total_Events"] = len(events) #Total Number of events
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut", "eta_cut")]
        cutflow["ReducedEvents"] = len(events)

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
        cutflow["ak4bJetsTight"] = ak.sum(ak.num(ak4_BJets_tight)) #No of ak4 tight bjets

        #Create Dijets
        def ObtainDiJets(jet):
            jet = jet[ak.num(jet)>1]
            Dijet = jet[:,0]+jet[:,1]
            return Dijet 
        DiJets = ObtainDiJets(ak4_BJets_tight)
        cutflow["bbDiJets"] = len(DiJets) #No of bb Dijets

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
            self.mode : {
                "Cutflow": cutflow ,
                "Histograms": {
                    "DiJet" : DiJetHist
                    }
                }
            }
        return output
    
    def postprocess(self, accumulator):
        pass
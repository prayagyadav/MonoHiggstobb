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

class SignalSignature(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        self._event_counter = 0 
        
    def process(self, events):
        dataset = events.metadata["dataset"]
        self.mode = dataset
        self._event_counter += len(events)
        cutflow = {}
        cutflow["Total_Events"] = len(events) #Total Number of events
        if cutflow["Total_Events"] >= 5000000:
            return output

        #Preparing histogram objects
        x_min = 0
        x_max = 1000
        nbins = 50
        DiJetHist = (
            hist.
            Hist.
            new.
            Reg(nbins,x_min,x_max).
            Double()
            )
        DiJetHistwithMETselection = (
            hist.
            Hist.
            new.
            Reg(nbins,x_min,x_max).
            Double()
            )

        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut", "eta_cut")]
        cutflow["ReducedEvents"] = len(events)

        if (self.mode).startswith("MET") :
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

        #MET Selection
        eventsMETcut = events[events.MET.pt > 200 ] #250GeV for boosted category
        
        Jets = events.Jet
        JetswMET = eventsMETcut.Jet
        #Apply the btag 
        btag_WP_medium = 0.3040 # Medium Working Point for 2018
        btag_WP_tight = 0.7476 # Tight Working Point for 2018
        GoodJetCut = Jets.btagDeepFlavB > btag_WP_tight 
        GoodJetCutwMET = JetswMET.btagDeepFlavB > btag_WP_tight 
        ak4_BJets_tight = Jets[GoodJetCut]
        ak4_BJets_tightwMET = JetswMET[GoodJetCutwMET]
        cutflow["ak4bJetsTight"] = ak.sum(ak.num(ak4_BJets_tight)) #No of ak4 tight bjets
        cutflow["ak4bJetsTight_with_MET_cut"] = ak.sum(ak.num(ak4_BJets_tightwMET)) #No of ak4 tight bjets with MET cut

        #Create Dijets
        def ObtainDiJets(jet):
            jet = jet[ak.num(jet)>1]
            Dijet = jet[:,0]+jet[:,1]
            return Dijet 
        DiJets = ObtainDiJets(ak4_BJets_tight)
        DiJetswMET = ObtainDiJets(ak4_BJets_tightwMET)
        cutflow["bbDiJets"] = len(DiJets) #No of bb Dijets
        cutflow["bbDiJets_with_MET_cut"] = len(DiJetswMET) #No of bb Dijets with MET cut

        #Fill the histogram
        DiJetHist.fill(DiJets.mass)
        DiJetHistwithMETselection.fill(DiJetswMET.mass)

        #Prepare the output
        output = {
            self.mode : {
                "Cutflow": cutflow ,
                "Histograms": {
                    "DiJet" : DiJetHist ,
                    "DiJetMETcut" : DiJetHistwithMETselection
                    }
                }
            }
        return output
    
    def postprocess(self, accumulator):
        pass
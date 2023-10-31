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
from coffea.lumi_tools import LumiMask
import hist

########################
# Define the processor #
########################

class SignalSignature(processor.ProcessorABC):
    """
    Flow of Data:

    INPUT EVENTS
        |
        |-------------------------------------------
        |                                           |
        |                                           |
        v                                           v
      if MET_Run2018                        else if MC
        |                                           |
        |                                           |
        |                                           |
        v                                           |
    MET TRIGGER                                     |
        |                                           |
        |                                           |
        |                                           |
        v                                           |
    MET FILTERS                                     |
        |                                           |
        |<------------------------------------------
        |                                           
        v
    MET CUT
        |
        |
        |
        v
    OBJECT SELECTION
        |
        |
        |
        v
    Make MET pt eta phi, DIJET mass etc plots - all of them plots
    """


    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        self.run_set = set({})

    def process(self, events):
        dataset = events.metadata["dataset"]
        self.mode = dataset

        cutflow = {}


        cutflow["Total_Events"] = len(events) #Total Number of events

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

        if (self.mode).startswith("MET") :

            #Selecting use-able events
            path = "../monoHbbtools/Load/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt"
            lumimask = LumiMask(path)
            events = events[lumimask(events.run, events.luminosityBlock)]
            cutflow["lumimask"] = len(events)

            #Saving the event run
            for run in set(events.run):
                self.run_set.add(run)

            #MET Triggers
            trigger = PackedSelection()
            trigger.add("noMuon", events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60 | events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight | events.HLT.PFMETNoMu140_PFMHTNoMu140_IDTight)

            trigger_cut = trigger.all("noMuon")

            events[trigger_cut]

            cutflow["triggered_events"] = len(events)

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

            cutflow["filtered_events"] = len(events)

        #MET Selection
        eventsMETcut = events[events.MET.pt > 200 ] #250GeV for boosted category

        #Object selections
        #ak4Jets

        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut", "eta_cut")]
        cutflow["Events_with_good_jets"] = len(events)
        
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
                    },
                "RunSet":self.run_set
                }
            }
        return output
    
    def postprocess(self, accumulator):
        pass
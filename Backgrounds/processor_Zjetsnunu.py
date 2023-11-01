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


#Define some selection functions to use in the processor
def loose_electrons(events):
    Etagap = ( events.Electron.eta < 1.4442 ) & ( events.Electron.eta > 1.566 )
    Eta = abs( events.Electron.eta ) < 2.5
    Pt = events.Electron.pt > 10 
    Id =events.Electron.cutBased >= 2 #meaning loose, medium or tight , ie , at least loosely an electron 
    return events.Electron[Etagap & Eta & Pt & Id]

def tight_electrons(events):
    Etagap = ( events.Electron.eta < 1.4442 ) & ( events.Electron.eta > 1.566 )
    Eta = abs( events.Electron.eta ) < 2.5
    Pt = events.Electron.pt > 40 
    Id =events.Electron.cutBased == 2 #meaning only tight electrons 
    return events.Electron[Etagap & Eta & Pt & Id]

def loose_muons(events):
    PFCand = events.Muon.isPFcand
    RelIso = events.Muon.pfRelIso04_all < 0.25
    Eta = abs(events.Muon.eta) < 2.4
    Pt = events.Muon.pt > 10
    Id = events.Muon.looseId 
    return events[PFCand & RelIso & Eta & Pt & Id]

def tight_muons(events):
    PFCand = events.Muon.isPFcand
    RelIso = events.Muon.pfRelIso04_all < 0.15
    Eta = abs(events.Muon.eta) < 2.4
    Pt = events.Muon.pt > 30
    Id = events.Muon.tightId 
    return events[PFCand & RelIso & Eta & Pt & Id]

def taus(events):
    #check the purpose of different variables used here
    Pt = events.Tau.pt > 20
    Eta = abs(events.Tau.eta) < 2.3
    decay = events.Tau.idDecayMode
    MVAid = events.Tau.idMVAoldDM2017v2 == 4 # Check if this means a tight tau
    AntiEle = events.Tau.idAntiEle >= 2 
    AntiMu = events.Tau.idAntiMu >= 1
    return events[Pt & Eta & decay & MVAid & AntiEle & AntiMu]

def loose_photons(events):
    Pt = events.Photon.pt > 20 
    Eta = abs(events.Photon.eta ) < 2.5
    Id = events.Photon.cutBased >= 1 #__doc = 0: fail  1: loose    2: medium   3: tight
    return events[Pt & Eta & Id]

#Begin the processor definition
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
        x_min = 100
        x_max = 150
        nbins = 25
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
        events_without_MET_selection = events
        events = events[events.MET.pt > 200 ] #200 for resolved category and 250GeV for boosted category
        cutflow["MET_greater_than_200GeV"] = len(events)
        
        #vetoes
        veto = PackedSelection()
        veto.add("noElectrons", ak.num( loose_electrons(events) ) == 0 )
        veto.add("noMuons", ak.num( loose_muons(events) ) == 0 )
        veto.add("noPhotons", ak.num( loose_photons(events) ) == 0 )
        veto.add("noTaus", ak.num( taus(events) ) == 0 )
        events = events[veto.all("noElectrons","noMuons","noPhotons","noTaus")]
        cutflow["No_Leptons_Photons"] = len(events)

        # Least number of jets and additional Jets
        events = events[ (ak.num(events.Jet) >= 2) & (ak.num(events.Jet) <=4 )] # 4 meaning 2 to construct dijet and 2 are the maximum number of additional jets
        cutflow["least_and_max_number_of_jets"] = len(events)

        #Object selections
        #ak4Jets

        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 30.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut", "eta_cut")]
        cutflow["Jets_eta_cut"] = len(events)
        
        Jets = events_without_MET_selection.Jet
        JetswMET = events.Jet
        cutflow["Number_of_jets"] = ak.sum(ak.num(events.Jet))

        #Anti-QCD DeltaPhi selection
        delta_phi = events.Jet.delta_phi(events.MET)
        events.Jet = events.Jet[abs(delta_phi) > 0.4]
        cutflow["Number_of_jets_after_antiQCD"] = ak.sum(ak.num(events.Jet))

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
            leading_jet = jet[:,0]
            leading_jet = leading_jet[leading_jet.pt > 50.0 ] #Leading Jet pt cut
            subleading_jet = jet[:,1]
            subleading_jet = subleading_jet[subleading_jet.pt > 30.0] #Subleading Jet pt cut (Redundant)
            Dijet = leading_jet + subleading_jet
            return Dijet 
        DiJets = ObtainDiJets(ak4_BJets_tight)
        Dijets = Dijets[( Dijets.mass > 100 ) & ( Dijets.mass < 150.0 ) ] #Dijet mass window cut
        Dijets = Dijets[Dijets.pt > 100.0 ] #Dijet pt cut
        DiJetswMET = ObtainDiJets(ak4_BJets_tightwMET)
        DijetswMET = DijetswMET[( DijetswMET.mass > 100 ) & ( DijetswMET.mass < 150.0 ) ] #Dijet mass window cut
        DiJetswMET = DiJetswMET[DiJetswMET.pt > 100.0 ] #Dijet pt cut
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
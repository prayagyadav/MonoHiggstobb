""" 
~~~~~~~~~~~~~~~~~~~~~~~PROCESSOR SCRIPT~~~~~~~~~~~~~~~~~~~~
This script prepares plots for my 9th sem presentation .

/Author/: Prayag Yadav
/Created/: 9 Nov 2023
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
    Pt = events.Electron.pt > 10.0 
    Id = events.Electron.cutBased >= 2 #meaning loose, medium or tight , ie , at least loosely an electron 
    return events.Electron[Etagap & Eta & Pt & Id]

def tight_electrons(events):
    Etagap = ( events.Electron.eta < 1.4442 ) & ( events.Electron.eta > 1.566 )
    Eta = abs( events.Electron.eta ) < 2.5
    Pt = events.Electron.pt > 40.0 
    Id = events.Electron.cutBased == 2 #meaning only tight electrons 
    return events.Electron[Etagap & Eta & Pt & Id]

def loose_muons(events):
    PFCand = events.Muon.isPFcand
    RelIso = events.Muon.pfRelIso04_all < 0.25
    Eta = abs(events.Muon.eta) < 2.4
    Pt = events.Muon.pt > 10.0
    Id = events.Muon.looseId 
    return events.Muon[PFCand & RelIso & Eta & Pt & Id]

def tight_muons(events):
    PFCand = events.Muon.isPFcand
    RelIso = events.Muon.pfRelIso04_all < 0.15
    Eta = abs(events.Muon.eta) < 2.4
    Pt = events.Muon.pt > 30.0
    Id = events.Muon.tightId 
    return events.Muon[PFCand & RelIso & Eta & Pt & Id]

def taus(events, version = 9):
    match version :
        case 7 :
            #check the purpose of different variables used here
            Pt = events.Tau.pt > 20.0
            Eta = abs(events.Tau.eta) < 2.3
            decay = events.Tau.decayMode
            MVAid = events.Tau.idMVAoldDM2017v2 == 4 # Check if this means a tight tau
            AntiEle = events.Tau.idAntiEle >= 2 
            AntiMu = events.Tau.idAntiMu >= 1
            return events.Tau[Pt & Eta & decay & MVAid & AntiEle & AntiMu]
        case 9 :
            #check the purpose of different variables used here
            Pt = events.Tau.pt > 20.0
            Eta = abs(events.Tau.eta) < 2.3
            decay = events.Tau.idDecayModeOldDMs
            #decay = events.Tau.idMVAoldDM2017v2 >=4) #for previous campaign
            AntiEle = events.Tau.idAntiEleDeadECal >= 2
            AntiMu = events.Tau.idAntiMu >= 1
            deepid1 = events.Tau.idDeepTau2017v2p1VSe >= 8 #for UL campaign and nanoAODv9
            deepid2 = events.Tau.idDeepTau2017v2p1VSmu >= 2
            deepid3 = events.Tau.idDeepTau2017v2p1VSjet >= 8
            return events.Tau[Pt & Eta & decay & deepid1 & deepid2 & deepid3 & AntiEle & AntiMu]



def loose_photons(events):
    Pt = events.Photon.pt > 20.0
    Eta = abs(events.Photon.eta ) < 2.5
    Id = events.Photon.cutBased >= 1 #__doc = 0: fail  1: loose    2: medium   3: tight
    return events.Photon[Pt & Eta & Id]

def lumi(events,cutflow):
    #Selecting use-able events
    path = "../monoHbbtools/Load/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt"
    lumimask = LumiMask(path)
    events = events[lumimask(events.run, events.luminosityBlock)]
    cutflow["lumimask"] = len(events)
    return events , cutflow

def met_trigger(events,cutflow):
    #MET Triggers
    trigger = PackedSelection()
    trigger.add(
        "noMuon",
        events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60 |
        events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight |
        events.HLT.PFMETNoMu140_PFMHTNoMu140_IDTight
        )
    trigger_cut = trigger.all("noMuon")
    events = events[trigger_cut]
    cutflow["MET trigger"] = len(events)
    return events , cutflow

def met_filter(events,cutflow):
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
    cutflow["MET filters"] = len(events)
    return events , cutflow

def met_selection(events,cutflow):
    #MET Selection
    events = events[events.MET.pt > 200 ] #200 for resolved category and 250GeV for boosted category
    cutflow["MET > 200 GeV"] = len(events)
    return events , cutflow

def no_electrons(events,cutflow):
    #no electron veto
    events = events[ak.num( loose_electrons(events) ) == 0] # no electrons
    cutflow["no electrons"] = len(events)
    return events , cutflow

def no_muons(events,cutflow):
    events = events[ak.num( loose_muons(events) ) == 0] # no muons
    cutflow["no muons"] = len(events)
    return events , cutflow

def no_photons(events,cutflow):
    events = events[ak.num( loose_photons(events) ) == 0] # no photons
    cutflow["no photons"] = len(events)
    return events,cutflow

def no_taus(events,cutflow, version=9):
    events = events[ak.num( taus(events, version=version) ) == 0]
    cutflow["no taus"] = len(events)
    return events , cutflow

def anti_QCD(events,cutflow):
    delta_phi = events.Jet.delta_phi(events.MET)
    events.Jet = events.Jet[abs(delta_phi) > 0.4]
    events = events[ak.num(events.Jet) > 0]
    cutflow["deltaphi(jets, MET) > 0.4"] = len(events)
    return events , cutflow

def jet_pt(events,cutflow):
    pt_cut = ak.all(events.Jet.pt > 30.0 , axis = 1)
    events = events[pt_cut]
    cutflow["jet pt > 30"] = len(events)
    return events , cutflow

def jet_eta(events,cutflow):
    eta_cut = ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1)
    events = events[eta_cut]
    cutflow["jet abs(eta) < 2.5"] = len(events)
    return events,cutflow

def get_bjets(events):
    #2018
    btag_WP_medium = 0.3040 # Medium Working Point for 2018
    btag_WP_tight = 0.7476 # Tight Working Point for 2018
    tight_bjets_selection = events.Jet.btagDeepFlavB > btag_WP_tight 
    return events.Jet[tight_bjets_selection]

def at_least_two_bjets(events,cutflow):
    bjets = get_bjets(events)
    events = events[ak.num(bjets) >= 2] #at least 2 bjets
    cutflow["At least two bjets"] = len(events)
    return events,cutflow

def leading_jet_pt(events,cutflow):
    ljets_cut = events.Jet[:,0].pt > 50.0 #Leading Jet pt cut
    events = events[ljets_cut]
    cutflow["bjet1 pt > 50 GeV "] = len(events)
    return events,cutflow

def subleading_jet_pt(events,cutflow):
    sjets_cut = events.Jet[:,1].pt > 30.0 #Subleading Jet pt cut 
    events = events[sjets_cut] 
    cutflow["bjet2 pt > 30 GeV"] = len(events)
    return events,cutflow

def additional_jets(events, cutflow):
    events = events[ak.num(events.Jet) <= 4] #Number of additional jets is 0, 1 or 2
    cutflow["Additional Jets <= 2"] = len(events)
    return events,cutflow

def dijet_mass(dijets,cutflow):
    dijets = dijets[( dijets.mass > 100.0 ) & ( dijets.mass < 150.0 ) ] #Dijet mass window cut
    cutflow["dijet mass between 100 Gev to 150 GeV"] = len(dijets) #No of bb Dijets is equal to the number of events passed
    return dijets, cutflow

def dijet_pt(dijets, cutflow):
    dijets = dijets[dijets.pt > 100.0 ] #Dijet pt cut
    cutflow["dijet pt > 100 GeV"] = len(dijets) #No of bb Dijets is equal to the number of events passed
    return dijets, cutflow

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
        cutflow["Total events"] = len(events) #Total Number of events

        #Preparing histogram objects
        #MET
        met_pt_min = 0
        met_pt_max = 1000
        met_pt_nbins = 25
        met_pt_hist = (
            hist.
            Hist.
            new.
            Reg(met_pt_nbins,met_pt_min,met_pt_max).
            Double()
        )
        met_phi_min = -3.14
        met_phi_max = 3.14
        met_phi_nbins = 15
        met_phi_hist = (
            hist.
            Hist.
            new.
            Reg(met_phi_nbins,met_phi_min,met_phi_max).
            Double()
        )
        #Leading ak4bjets
        leadingjets_pt_min = 0
        leadingjets_pt_max = 500
        leadingjets_pt_nbins = 20
        leadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_pt_nbins,leadingjets_pt_min,leadingjets_pt_max).
            Double()
        )
        leadingjets_eta_min = -3.0
        leadingjets_eta_max = 3.0
        leadingjets_eta_nbins = 15
        leadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_eta_nbins,leadingjets_eta_min,leadingjets_eta_max).
            Double()
        )
        leadingjets_phi_min = -3.14
        leadingjets_phi_max = 3.14
        leadingjets_phi_nbins = 15
        leadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_phi_nbins,leadingjets_phi_min,leadingjets_phi_max).
            Double()
        )
        leadingjets_mass_min = 0
        leadingjets_mass_max = 500
        leadingjets_mass_nbins = 20
        leadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_mass_nbins,leadingjets_mass_min,leadingjets_mass_max).
            Double()
        )
        #Subleading ak4 bjets
        subleadingjets_pt_min = 0
        subleadingjets_pt_max = 500
        subleadingjets_pt_nbins = 20
        subleadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_pt_nbins,subleadingjets_pt_min,subleadingjets_pt_max).
            Double()
        )
        subleadingjets_eta_min = -3.0
        subleadingjets_eta_max = 3.0
        subleadingjets_eta_nbins = 15
        subleadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_eta_nbins,subleadingjets_eta_min,subleadingjets_eta_max).
            Double()
        )
        subleadingjets_phi_min = -3.14
        subleadingjets_phi_max = 3.14
        subleadingjets_phi_nbins = 15
        subleadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_phi_nbins,subleadingjets_phi_min,subleadingjets_phi_max).
            Double()
        )
        subleadingjets_mass_min = 0
        subleadingjets_mass_max = 500
        subleadingjets_mass_nbins = 20
        subleadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_mass_nbins,subleadingjets_mass_min,subleadingjets_mass_max).
            Double()
        )
        #ak4bjet-ak4bjet dijets 
        dijets_pt_min = 0
        dijets_pt_max = 500
        dijets_pt_nbins = 20
        dijets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_pt_nbins,dijets_pt_min,dijets_pt_max).
            Double()
            )
        dijets_eta_min = -3.0
        dijets_eta_max = 3.0
        dijets_eta_nbins = 15
        dijets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_eta_nbins,dijets_eta_min,dijets_eta_max).
            Double()
            )
        dijets_phi_min = -3.14
        dijets_phi_max = 3.14
        dijets_phi_nbins = 15
        dijets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_phi_nbins,dijets_phi_min,dijets_phi_max).
            Double()
            )
        dijets_mass_min = 100
        dijets_mass_max = 150
        dijets_mass_nbins = 20
        dijets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_mass_nbins,dijets_mass_min,dijets_mass_max).
            Double()
            )

        if (self.mode).startswith("MET") :

            #choosing certified good events
            events, cutflow = lumi(events,cutflow)

            #Saving the event run
            for run in set(events.run):
                self.run_set.add(run)

            #MET_Trigger
            events, cutflow = met_trigger(events,cutflow)

            #MET_Filters
            events, cutflow = met_filter(events,cutflow)
            

        #MET_selection
        events, cutflow = met_selection(events,cutflow)
        
        #vetoes
        # events, cutflow = no_electrons(events,cutflow)
        # events, cutflow = no_muons(events,cutflow)
        # events, cutflow = no_photons(events,cutflow)
        # if (self.mode).startswith("MonoHTobb_ZpBaryonic"):
        #     events, cutflow = no_taus(events,cutflow, version=7)
        # else :
        #     events, cutflow = no_taus(events,cutflow, version=9)

        #Object selections
        #ak4Jets

        #Apply pt and eta cut
        events, cutflow = jet_pt(events,cutflow)
        events, cutflow = jet_eta(events,cutflow)

        #Anti-QCD DeltaPhi selection
        # events, cutflow = anti_QCD(events,cutflow)

        #Apply the btag 
        bjets = get_bjets(events)
        
        #At least two bjets
        events, cutflow = at_least_two_bjets(events,cutflow)

        #Create Dijets
        #leading bjet pt
        # events, cutflow = leading_jet_pt(events,cutflow)
        # #subleading bjet pt
        # events, cutflow = subleading_jet_pt(events,cutflow)

        #At most 2 additional jets
        # events, cutflow = additional_jets(events, cutflow)

        leading_jets = events.Jet[:,0]
        subleadingjets = events.Jet[:,1]
        dijets = events.Jet[:,0] + events.Jet[:,1] #Leading jet + Subleading jet

        #Dijet mass window
        # dijets , cutflow = dijet_mass(dijets,cutflow)

        #Dijet pt
        # dijets, cutflow = dijet_pt(dijets,cutflow)

        #Fill the histogram
        #MET
        met_pt_hist.fill(events.MET.pt)
        met_phi_hist.fill(events.MET.phi)
        #Leading jets
        leadingjets_pt_hist.fill(leading_jets.pt)
        leadingjets_eta_hist.fill(leading_jets.eta)
        leadingjets_phi_hist.fill(leading_jets.phi)
        leadingjets_mass_hist.fill(leading_jets.mass)
        #Subleading jets
        subleadingjets_pt_hist.fill(subleadingjets.pt)
        subleadingjets_eta_hist.fill(subleadingjets.eta)
        subleadingjets_phi_hist.fill(subleadingjets.phi)
        subleadingjets_mass_hist.fill(subleadingjets.mass)
        #ak4bjet-ak4bjet dijets
        dijets_pt_hist.fill(dijets.pt)
        dijets_eta_hist.fill(dijets.eta)
        dijets_phi_hist.fill(dijets.phi)
        dijets_mass_hist.fill(dijets.mass)
        

        #Prepare the output
        key_list = ["MET_Run2018","MonoHTobb_ZpBaryonic"]
        for key in key_list :
            if self.mode.startswith("MET_Run2018") :
                self.key = key
            if self.mode.startswith("MonoHTobb_ZpBaryonic") :
                self.key = key

        output = {
            self.mode : {
                self.mode : {
                    "Cutflow": cutflow ,
                    "Histograms": {
                        "met_pt_hist" : met_pt_hist ,
                        "met_phi_hist" : met_phi_hist ,
                        "leadingjets_pt_hist" : leadingjets_pt_hist ,
                        "leadingjets_eta_hist" : leadingjets_eta_hist ,
                        "leadingjets_phi_hist" : leadingjets_phi_hist ,
                        "leadingjets_mass_hist" : leadingjets_mass_hist ,
                        "subleadingjets_pt_hist" : subleadingjets_pt_hist ,
                        "subleadingjets_eta_hist" : subleadingjets_eta_hist ,
                        "subleadingjets_phi_hist" : subleadingjets_phi_hist ,
                        "subleadingjets_mass_hist" : subleadingjets_mass_hist ,
                        "dijets_pt" : dijets_pt_hist ,
                        "dijets_eta" : dijets_eta_hist ,
                        "dijets_phi" : dijets_phi_hist ,
                        "dijets_mass" : dijets_mass_hist ,
                        },
                    "RunSet":self.run_set
                    }
                }
            }
        return output
    
    def postprocess(self, accumulator):
        pass
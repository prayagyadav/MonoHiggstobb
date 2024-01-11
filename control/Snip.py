import awkward as ak
from coffea.analysis_tools import PackedSelection
from coffea.lumi_tools import LumiMask

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

def tight_muons(events, cutflow): ####
    # events = Events
    # PFCand = events.Muon.isPFcand
    # RelIso = events.Muon.pfRelIso04_all < 0.15
    # Eta = abs(events.Muon.eta) < 2.4
    # Pt = events.Muon.pt > 30.0
    # Id = events.Muon.tightId 

    # final_cut = PFCand & RelIso & Eta & Pt & Id
    # print("initial events: ", len(events))
    # Tightmuons = events.Muon[final_cut]
    # print("Tightmuon pt : \n",Tightmuons.pt)
    # events = events[ak.num(Tightmuons) == 1]
    # print("are these really single? \n", events.Muon.pt)
    
    PFCand = events.Muon.isPFcand
    RelIso = events.Muon.pfRelIso04_all < 0.15
    Eta = abs(events.Muon.eta) < 2.4
    Pt = events.Muon.pt > 30.0
    Id = events.Muon.tightId 
    sel = PackedSelection()
    sel.add("tight", ak.all(PFCand & RelIso & Eta & Pt & Id , axis = 1))
    sel.add("one",ak.num(events.Muon.pt) == 1)
    events = events[sel.all("tight","one")]
    cutflow["one_tight_muon"] = len(events)
    return events , cutflow

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

def lumi(events,cutflow,path="",lumiobject=None):
    #Selecting use-able events
    if lumiobject==None :
        #path_of_file = path+"Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.json"
        path_of_file = path+"golden.json"
        lumimask = LumiMask(path_of_file)
    else :
        lumimask = lumiobject
    events = events[lumimask(events.run, events.luminosityBlock)]
    cutflow["lumimask"] = len(events)
    return events , cutflow

def met_trigger(events,cutflow,era): ####
    #MET Triggers
    trigger = PackedSelection()
    if era == 2018:
        trigger.add(
            "noMuonEnergyInJet",
            events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight_PFHT60 |
            events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight |
            events.HLT.PFMETNoMu140_PFMHTNoMu140_IDTight 
            )
    elif era == 2017 :
        trigger.add(
            "noMuonEnergyInJet",
            events.HLT.PFMETNoMu120_PFMHTNoMu120_IDTight
        )
    trigger_cut = trigger.all("noMuonEnergyInJet")
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

def met_selection(events,cutflow,GeV):
    #MET Selection
    events = events[events.MET.pt > GeV ] #200 for SR resolved category and 250GeV for SR boosted category
    cutflow[f"MET > {GeV} GeV"] = len(events)
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

def get_bjets(events, wp="tight", era=2018):
    if era==2018:
        #2018
        if wp=="loose":
            raise KeyError
        elif wp=="medium":
            btag_WP = 0.3040 # Medium Working Point for 2018
        elif wp=="tight":
            btag_WP = 0.7476 # Tight Working Point for 2018
    if era==2017:
        #2017
        if wp=="loose":
            raise KeyError
        elif wp=="medium":
            raise KeyError
        elif wp=="tight":
            raise KeyError
    tight_bjets_selection = events.Jet.btagDeepFlavB > btag_WP 
    return events.Jet[tight_bjets_selection]

def at_least_two_bjets(events,cutflow,year):
    bjets = get_bjets(events, wp="tight",era=year)
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

def additional_ak4_jets(events, cutflow , cat, comparator="equal_to", number = 1):
    if cat=="boosted":
        n_essential_ak4_jets = 0
    elif cat=="resolved":
        n_essential_ak4_jets = 2
    if comparator=="equal_to" :
        events = events[ak.num(events.Jet) == (n_essential_ak4_jets + number)]
    elif comparator=="greater_than_or_equal_to":
        events = events[ak.num(events.Jet) >= (n_essential_ak4_jets + number)]
    elif comparator=="less_than_or_equal_to":
        events = events[ak.num(events.Jet) <= (n_essential_ak4_jets + number)]
    cutflow[f"Additional Jets {comparator} {number}"] = len(events)
    return events,cutflow

def dijet_mass(events,cutflow,Dijets,window):
    """
    Warning: Use only when number of dijets is equal to the number of events
    """
    window_cut = ( Dijets.mass > window[0] ) & ( Dijets.mass < window[1] ) #Dijet mass window cut
    events = events[window_cut] # Allowed since no. of dijets is equal to number of events
    Dijets = Dijets[window_cut]
    cutflow["dijet mass between 100 Gev to 150 GeV"] = len(events) 
    return events, cutflow, Dijets

def dijet_pt(events,cutflow,Dijets,pt):
    """
    Warning: Use only when number of dijets is equal to the number of events
    """
    pt_cut = Dijets.pt > pt #Dijet pt cut 
    events = events[pt_cut]
    Dijets = Dijets[pt_cut]
    cutflow["dijet pt > 100 GeV"] = len(events) #No of bb Dijets is equal to the number of events passed
    return events, cutflow, Dijets
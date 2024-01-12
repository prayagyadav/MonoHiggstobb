from coffea import processor
import hist 
from Snip import *
#Begin the processor definition
class Top_mu(processor.ProcessorABC):
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
    EVENT SELECTION AND OBJECT SELECTION
        |
        |
        |
        v
    Make MET pt eta phi, DIJET mass etc plots - all of them plots
    """


    def __init__(self,category,helper_objects = [] ):
        # Initialize the cutflow dictionary
        if len(helper_objects) > 0 :
            self.lumiobject = helper_objects[0] 
        self.category = category
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
        met_pt_nbins = 50
        met_pt_hist = (
            hist.
            Hist.
            new.
            Reg(met_pt_nbins,met_pt_min,met_pt_max).
            Double()
        )
        met_phi_min = -3.14
        met_phi_max = 3.14
        met_phi_nbins = 6
        met_phi_hist = (
            hist.
            Hist.
            new.
            Reg(met_phi_nbins,met_phi_min,met_phi_max).
            Double()
        )
        #Leading ak4bjets
        leadingjets_pt_min = 0
        leadingjets_pt_max = 1000
        leadingjets_pt_nbins = 50
        leadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_pt_nbins,leadingjets_pt_min,leadingjets_pt_max).
            Double()
        )
        leadingjets_eta_min = -2.5
        leadingjets_eta_max = 2.5
        leadingjets_eta_nbins = 5
        leadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_eta_nbins,leadingjets_eta_min,leadingjets_eta_max).
            Double()
        )
        leadingjets_phi_min = -3.14
        leadingjets_phi_max = 3.14
        leadingjets_phi_nbins = 6
        leadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_phi_nbins,leadingjets_phi_min,leadingjets_phi_max).
            Double()
        )
        leadingjets_mass_min = 0
        leadingjets_mass_max = 1000
        leadingjets_mass_nbins = 50
        leadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(leadingjets_mass_nbins,leadingjets_mass_min,leadingjets_mass_max).
            Double()
        )
        #Subleading ak4 bjets
        subleadingjets_pt_min = 0
        subleadingjets_pt_max = 1000
        subleadingjets_pt_nbins = 50
        subleadingjets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_pt_nbins,subleadingjets_pt_min,subleadingjets_pt_max).
            Double()
        )
        subleadingjets_eta_min = -2.5
        subleadingjets_eta_max = 2.5
        subleadingjets_eta_nbins = 5
        subleadingjets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_eta_nbins,subleadingjets_eta_min,subleadingjets_eta_max).
            Double()
        )
        subleadingjets_phi_min = -3.14
        subleadingjets_phi_max = 3.14
        subleadingjets_phi_nbins = 6
        subleadingjets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_phi_nbins,subleadingjets_phi_min,subleadingjets_phi_max).
            Double()
        )
        subleadingjets_mass_min = 0
        subleadingjets_mass_max = 1000
        subleadingjets_mass_nbins = 50
        subleadingjets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(subleadingjets_mass_nbins,subleadingjets_mass_min,subleadingjets_mass_max).
            Double()
        )
        #ak4bjet-ak4bjet dijets 
        dijets_pt_min = 0
        dijets_pt_max = 1000
        dijets_pt_nbins = 50
        dijets_pt_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_pt_nbins,dijets_pt_min,dijets_pt_max).
            Double()
            )
        dijets_eta_min = -2.5
        dijets_eta_max = 2.5
        dijets_eta_nbins = 5
        dijets_eta_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_eta_nbins,dijets_eta_min,dijets_eta_max).
            Double()
            )
        dijets_phi_min = -3.14
        dijets_phi_max = 3.14
        dijets_phi_nbins = 6
        dijets_phi_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_phi_nbins,dijets_phi_min,dijets_phi_max).
            Double()
            )
        dijets_mass_min = 100
        dijets_mass_max = 150
        dijets_mass_nbins = 5
        dijets_mass_hist = (
            hist.
            Hist.
            new.
            Reg(dijets_mass_nbins,dijets_mass_min,dijets_mass_max).
            Double()
            )

        if (self.mode).startswith("MET") :

            #choosing certified good events
            #figure out how to implement condor folder transfers and fix this, in the meantime ignore for condor runs
            should_lumi = True
            if should_lumi :
                events, cutflow = lumi(events, cutflow, lumiobject=self.lumiobject)

            #Saving the event run
            for run in set(events.run):
                self.run_set.add(run)

            #MET_Trigger
            events, cutflow = met_trigger(events,cutflow,era=2018)

            #MET_Filters
            events, cutflow = met_filter(events,cutflow)
        

        #MET_selection
        events, cutflow = met_selection(events,cutflow,GeV=50.0)
    
        #vetoes
        events, cutflow = no_electrons(events,cutflow)
        events, cutflow = no_photons(events,cutflow)
        if (self.mode).startswith("MonoHTobb_ZpBaryonic"):
            events, cutflow = no_taus(events,cutflow, version=7)
        else :
            events, cutflow = no_taus(events,cutflow, version=9)

        #Object selections
            
        #Exactly one tight muon
        events, cutflow = tight_muons(events,cutflow)

        #Hadronic Recoil
        #There is at least one muon(tight) in each event at this point
        events.HET = events.MET.pt - events.Muon.pt
        if self.category == "boosted" :
            events = events[events.HET > 250.0 ]
        elif self.category == "resolved" :
            events = events[events.HET > 200.0 ]
        cutflow["Hadronic Recoil"] = len(events)

        #ak4Jets
        #Apply general pt and eta cut to jets
        events, cutflow = jet_pt(events,cutflow)
        events, cutflow = jet_eta(events,cutflow)
    
        #Apply the btag and choose at least two bjet events
        events, cutflow = at_least_two_bjets(events,cutflow,year=2018)

        # leading bjet pt
        events, cutflow = leading_jet_pt(events,cutflow)
        #subleading bjet pt
        events, cutflow = subleading_jet_pt(events,cutflow)

        if self.category == "resolved":
            #At least 1 additional jets
            events, cutflow = additional_ak4_jets(events, cutflow, cat=self.category , comparator="greater_than_or_equal_to",number=1)
        elif self.category == "boosted":
            #At most 2 additional jets
            events, cutflow = additional_ak4_jets(events, cutflow, cat=self.category , comparator="less_than_or_equal_to",number=2)
        else:
            raise KeyError

        #Create Dijets
        leading_jets = events.Jet[:,0]
        subleadingjets = events.Jet[:,1]
        dijets = events.Jet[:,0] + events.Jet[:,1] #Leading jet + Subleading jet

        #Note: At this stage each dijet correspond to an event

        #Dijet mass window
        events, cutflow, dijets = dijet_mass(events,cutflow,dijets,window=[100.0,150.0])

        #Dijet pt
        events, cutflow, dijets = dijet_pt(events,cutflow,dijets,pt=100.0)

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
        key_list = [
        "MET_Run2018",
        "ZJets_NuNu",
        "TTToSemiLeptonic",
        "TTTo2L2Nu",
        "TTToHadronic",
        "WJets_LNu",
        "DYJets_LL",
        "VV",
        "QCD",
        "ST"
        ]
        if self.mode.startswith("MET_Run2018") :
            self.key = key_list[0]
        elif "Jets_NuNu" in self.mode :
            self.key = key_list[1]
        elif self.mode.startswith("TTToSemiLeptonic"):
            self.key = key_list[2]
        elif self.mode.startswith("TTTo2L2Nu"):
            self.key = key_list[3]
        elif self.mode.startswith("TTToHadronic"):
            self.key = key_list[4]
        elif self.mode.startswith("WJets_LNu"):
            self.key = key_list[5]
        elif self.mode.startswith("DYJets_LL"):
            self.key = key_list[6]
        elif ( self.mode.startswith("WW") | self.mode.startswith("WZ") | self.mode.startswith("ZZ") ) :
            self.key = key_list[7]
        elif self.mode.startswith("QCD"):
            self.key = key_list[8]
        elif self.mode.startswith("ST"):
            self.key = key_list[9]
        else :
            print("Unidentified dataset ", self.mode)
            raise KeyError
    

        output = {
            self.key : {
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
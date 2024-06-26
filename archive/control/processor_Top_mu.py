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
        # process everything according to their recoil window
        def process_by_recoil_window(self,events,recoil_window):
            dataset = events.metadata["dataset"]
            self.mode = dataset
            self.recoil_window , self.mass_nbin = recoil_window 
            cutflow = {}
            cutflow["Total events"] = len(events) #Total Number of events

            #Preparing histogram objects
            #MET
            met_pt_min = 0
            met_pt_max = 500
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
            leadingjets_pt_max = 500
            leadingjets_pt_nbins = 25
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
            leadingjets_mass_max = 500
            leadingjets_mass_nbins = 25
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
            subleadingjets_pt_nbins = 25
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
            subleadingjets_mass_max = 500
            subleadingjets_mass_nbins = 25
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
            dijets_pt_nbins = 25
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
            dijets_mass_nbins = self.mass_nbin
            dijets_mass_hist = (
                hist.
                Hist.
                new.
                Reg(dijets_mass_nbins,dijets_mass_min,dijets_mass_max).
                Double()
                )

            if (self.mode).startswith("MET") :

                #choosing certified good events
                should_lumi = False
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

            #HEM veto
            events,cutflow = HEM_veto(events,cutflow)

            #Object selections
                
            #Exactly one tight muon
            events, single_muons, cutflow = tight_muons(events,cutflow)

            #Hadronic Recoil
            #There is at least one muon(tight) in each event at this point
            events.Recoil = events.MET.pt + ak.flatten(single_muons.pt)
            # if self.category == "boosted" :
            #     recoil = 250.0
            # elif self.category == "resolved" :
            #     recoil = 200.0
            windowcut = (events.Recoil > self.recoil_window[0]) & (events.Recoil < self.recoil_window[1])
            events = events[windowcut]
            cutflow["Recoil"] = len(events)

            #ak4Jets
            # #Apply general pt and eta cut to jets
            # events, cutflow = jet_pt(events,cutflow)
            # events, cutflow = jet_eta(events,cutflow)

            #Making sure that at least one jet is present before moving forward
            events = events[ak.num(events.Jet) > 0]
        
            #Apply the btag and choose at least two bjet events
            events, cutflow = at_least_two_bjets(events,cutflow,year=2018,Working_Point="medium")

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
        

            output_by_recoil_window = {
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
            return output_by_recoil_window
        
        ##########################################################################
        #Implementing the different regions
        recoils = {
            1: ([200,250],18),
            2: ([250,290],11),
            3: ([290,360],8),
            4: ([360,420],4),
            5: ([420,1000],3)
        }
        output = {}
        output["Recoil:200-250"] = process_by_recoil_window(self,events,recoils[1])
        output["Recoil:250-290"] = process_by_recoil_window(self,events,recoils[2])
        output["Recoil:290-360"] = process_by_recoil_window(self,events,recoils[3])
        output["Recoil:360-420"] = process_by_recoil_window(self,events,recoils[4])
        output["Recoil:420-1000"] = process_by_recoil_window(self,events,recoils[5])
        return output

    def postprocess(self, accumulator):
        pass

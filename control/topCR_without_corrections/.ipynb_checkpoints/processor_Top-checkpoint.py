import hist
from coffea import processor
from snippets import *

#Begin the processor definition
class Top(processor.ProcessorABC):
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
    Make MET pt eta phi, DIJET mass/softdrop mass etc plots - all of them plots
    """


    def __init__(self,category,lepton,helper_objects = [] ):
        # Initialize the cutflow dictionary
        if len(helper_objects) > 0 :
            self.lumiobject = helper_objects[0] 
        self.category = category
        self.lepton = lepton
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
            if self.category=="resolved":
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
                leadingjets_mass_max = 300
                leadingjets_mass_nbins = self.mass_nbin
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
                subleadingjets_mass_max = 300
                subleadingjets_mass_nbins = self.mass_nbin
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
            elif self.category=="boosted":
                pass

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
            

            #vetoes
            if self.lepton == "mu":
                events, cutflow = no_electrons(events,cutflow)
            elif self.lepton == "e":
                events, cutflow = no_muons(events,cutflow)
            if (self.mode).startswith("MonoHTobb_ZpBaryonic"):
                events, cutflow = no_taus(events,cutflow, version=7)
            else :
                events, cutflow = no_taus(events,cutflow, version=9)
            events, cutflow = no_photons(events,cutflow)

            #HEM veto
            events,cutflow = HEM_veto_bril(events,cutflow,cat=self.category)

            #Object selections
            if self.lepton == "mu":
                #Exactly one tight muon
                events, single_muons, cutflow = single_tight_muons(events,cutflow)
            elif self.lepton == "e":
                #Exactly one tight electron
                events, single_electrons, cutflow = single_tight_electrons(events,cutflow)

            #MET_selection
            events, cutflow = met_selection(events,cutflow,GeV=50.0)

            dummy_dict = {}
            if self.lepton=="mu":
                dummy_events, single_muons, dummy_dict = single_tight_muons(events,dummy_dict) # I just want to get tight muons back, so I use the same function with some dummy variables
            elif self.lepton=="e":
                dummy_events, single_electrons, dummy_dict = single_tight_electrons(events,dummy_dict) # I just want to get tight electrons back, so I use the same function with some dummy variables
            #Hadronic Recoil
            #There is at least one muon(tight) in each event at this point
            #events.Recoil = events.MET.pt + ak.flatten(single_muons.pt)
            # if self.category == "boosted" :
            #     recoil = 250.0
            # elif self.category == "resolved" :
            #     recoil = 200.0
            if self.lepton == "mu":
                Recoil = get_recoil(events,single_muons)
            elif self.lepton == "e":
                Recoil = get_recoil(events,single_electrons)
            windowcut = (Recoil > self.recoil_window[0]) & (Recoil < self.recoil_window[1])
            events = events[windowcut]
            cutflow["Recoil"] = len(events)

            #Do the calculations
            if self.category=="resolved":
                wp = get_wp(Era=2018,wp="medium")
                is_bjet = events.Jet.btagDeepFlavB > wp
                bjets = events.Jet[is_bjet]
            
                at_least_two_such_jets = ak.num(bjets, axis=1) >= 2
                bjets = bjets[at_least_two_such_jets]
            
                leading_bjets = bjets[:,0]
                leading_jet_pt_at_least_50 = leading_bjets.pt > 50
                bjets = bjets[leading_jet_pt_at_least_50 ]
            
                subleading_bjets = bjets[:,1]
                subleading_jet_pt_at_least_30 = subleading_bjets.pt > 30
                bjets = bjets[subleading_jet_pt_at_least_30]
            
                dijets = bjets[:,0]+bjets[:,1]

                dijets_pt_selection = dijets.pt > 100
                dijets = dijets[dijets_pt_selection]
            
                dijets_mass_window = (dijets.mass < 150) & (dijets.mass > 100)
                dijets = dijets[dijets_mass_window]
            
                #Reobtain the leading and subleading jets after the dijet cuts
                leadingjets = bjets[:,0]
                leadingjets = leadingjets[dijets_pt_selection]
                leadingjets = leadingjets[dijets_mass_window]

                subleadingjets = bjets[:,1]
                subleadingjets = subleadingjets[dijets_pt_selection]
                subleadingjets = subleadingjets[dijets_mass_window]

                #Updating the cutflow for all the above processes
                events = events[at_least_two_such_jets]
            
                events = events[leading_jet_pt_at_least_50]
                cutflow["leading jet pt > 50"] = len(events)
            
                events = events[subleading_jet_pt_at_least_30]
                cutflow["subleading jet pt > 30"] = len(events)

                events = events[dijets_pt_selection]
                cutflow["pt(bb) > 100"] = len(events)
            
                events = events[dijets_mass_window]
                cutflow["100 < M_bb < 150"] = len(events)

                one_normal_additional_jet = ak.num(events.Jet) >= 3
                events = events[one_normal_additional_jet]
                pt_greater_than30 = events.Jet.pt[:,2] > 30
                acceptable_eta = abs(events.Jet.eta[:,2] ) < 2.5
                events = events[pt_greater_than30 & acceptable_eta]
                cutflow["At least one normal additional jet"] = len(events)
            elif self.category=="boosted":
                pass
            
            #Fill the histogram
            #MET
            met_pt_hist.fill(events.MET.pt)
            met_phi_hist.fill(events.MET.phi)
            if self.category=="resolved":

                #Leading jets
                leadingjets_pt_hist.fill(leadingjets.pt)
                leadingjets_eta_hist.fill(leadingjets.eta)
                leadingjets_phi_hist.fill(leadingjets.phi)
                leadingjets_mass_hist.fill(leadingjets.mass)
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
            elif self.category=="boosted":
                pass

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
        
            if self.category=="resolved":
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
            elif self.category=="boosted":
                pass

            return output_by_recoil_window
        
        ##########################################################################
        #Implementing the different regions
        if self.category == "resolved":

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
        elif self.category == "boosted":
        
            recoils = {
                1: ([250,350],12),
                2: ([350,500],10),
                3: ([500,1000],3),
            }
            output = {}
            output["Recoil:250-350"] = process_by_recoil_window(self,events,recoils[1])
            output["Recoil:350-500"] = process_by_recoil_window(self,events,recoils[2])
            output["Recoil:500-1000"] = process_by_recoil_window(self,events,recoils[3])
        return output

    def postprocess(self, accumulator):
        pass

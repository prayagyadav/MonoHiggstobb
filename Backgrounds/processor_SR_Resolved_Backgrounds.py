""" 
~~~~~~~~~~~~~~~~~~~~~~~PROCESSOR SCRIPT~~~~~~~~~~~~~~~~~~~~
This script studies the background .

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
from monoHbbtools.Snip import *

########################
# Define the processor #
########################

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
            ignore = True
            if ignore :
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
        events, cutflow = no_electrons(events,cutflow)
        events, cutflow = no_muons(events,cutflow)
        events, cutflow = no_photons(events,cutflow)
        if (self.mode).startswith("MonoHTobb_ZpBaryonic"):
            events, cutflow = no_taus(events,cutflow, version=7)
        else :
            events, cutflow = no_taus(events,cutflow, version=9)

        #Object selections
        #ak4Jets

        #Apply pt and eta cut
        events, cutflow = jet_pt(events,cutflow)
        events, cutflow = jet_eta(events,cutflow)

        #Anti-QCD DeltaPhi selection
        events, cutflow = anti_QCD(events,cutflow)

        #Apply the btag 
        bjets = get_bjets(events)
        
        #At least two bjets
        events, cutflow = at_least_two_bjets(events,cutflow)

        #Create Dijets
        # leading bjet pt
        events, cutflow = leading_jet_pt(events,cutflow)
        #subleading bjet pt
        events, cutflow = subleading_jet_pt(events,cutflow)

        #At most 2 additional jets
        events, cutflow = additional_jets(events, cutflow)

        leading_jets = events.Jet[:,0]
        subleadingjets = events.Jet[:,1]
        dijets = events.Jet[:,0] + events.Jet[:,1] #Leading jet + Subleading jet

        #Dijet mass window
        dijets , cutflow = dijet_mass(dijets,cutflow)

        #Dijet pt
        dijets, cutflow = dijet_pt(dijets,cutflow)

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

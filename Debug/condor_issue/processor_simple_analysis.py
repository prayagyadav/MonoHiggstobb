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

class JetKinem(processor.ProcessorABC):
    def __init__(self):
        # Initialize the cutflow dictionary
        self.cutflow = {}
        pass
    def process(self, events):
        self.cutflow["Total_Events"] = len(events) #Total Number of events
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis = 1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 , axis = 1))
        events = events[BasicCuts.all("pt_cut")]
        Jets = events.Jet
        self.cutflow["ReducedJets"] = ak.sum(ak.num(Jets)) #No of jets passing the BasicCuts

        #Apply the btag 
        btag_WP_medium = 0.3040 # Medium Weight Parameter
        GoodJetCut = Jets.btagDeepFlavB > btag_WP_medium 
        ak4_BJets_med = Jets[GoodJetCut]
        self.cutflow["ak4bJetsMedium"] = ak.sum(ak.num(ak4_BJets_med)) #No of ak4 medium bjets

        #Create Dijets
        def ObtainDiJets(jet, bjet):
            jet = jet[ak.num(jet)>1]
            bjet = bjet[ak.num(bjet)>1]
            Dijet = jet[:,0]+jet[:,1]
            Dibjet = bjet[:,0]+bjet[:,1]
            return Dijet , Dibjet
        DiJets , DiJets_bb = ObtainDiJets(Jets, ak4_BJets_med)
        self.cutflow["DiJets"] = len(DiJets) #No of Dijets
        self.cutflow["bbDiJets"] = len(DiJets_bb) #No of bb Dijets

        #Create the histograms
        #1. bTag score histogram
        TagHist = hist.Hist.new.Reg(20,0.,1.).Double().fill(ak.flatten(Jets.btagDeepFlavB))

        #2. Jets pt : Untagged and Tagged 
        JetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(50,0.,500., name="pt", label="$p_t$ (GeV)")
            .Double()
            )
        JetHist.fill(Type="Untagged", pt = ak.flatten(Jets.pt))
        JetHist.fill(Type="btagDeepFlavB", pt = ak.flatten(ak4_BJets_med.pt))

        #3. DiJets Mass : Untagged and Tagged
        DiJetHist= (
            hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type")
            .Reg(50,0.,500., name="mass", label="Mass (GeV)")
            .Double()
            )
        DiJetHist.fill(Type="Untagged", mass = DiJets.mass)
        DiJetHist.fill(Type="btagDeepFlavB", mass = DiJets_bb.mass)

        #4. MET histogram
        metHist = (
            hist.Hist.new.Reg(100,0,500).Double()
        )
        metHist.fill(events.MET.pt)

        #Prepare the output
        output = {
            "Cutflow": self.cutflow ,
            "Histograms": {
                "Tag" : TagHist ,
                "Jetpt" : JetHist ,
                "DiJetMass" : DiJetHist ,
                "MET": metHist 
            }
        }
        return output
    def postprocess(self, accumulator):
        pass

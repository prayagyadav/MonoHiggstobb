from monoHbbtools.Utilities import condor 
from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
from coffea import processor
from coffea.analysis_tools import PackedSelection
import awkward as ak
import hist
from matplotlib import pyplot as plt
import mplhep as hep
import numpy as np

events = NanoEventsFactory.from_root(
    "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
    schemaclass=NanoAODSchema.v7,
    metadata={"Dataset":"MET_Run2018"}
    ).events()

class JetKinem(processor.ProcessorABC):
    def __init__(self):
        pass
    def process(self, events):
        #Apply the basic cuts like pt and eta
        BasicCuts = PackedSelection()
        BasicCuts.add("pt_cut", ak.all(events.Jet.pt > 25.0 , axis=1))
        BasicCuts.add("eta_cut", ak.all(abs( events.Jet.eta ) < 2.5 ,axis=1))
        events = events[BasicCuts.all("pt_cut","eta_cut")]
        Jets = events.Jet

        JetHist= (
            hist.Hist.new
            .Reg(100,0.,400., label="$p_t$ (GeV)")
            .Double()
            )
        JetHist.fill(ak.flatten(Jets.pt))

        #Prepare the output
        output = {
            "Histograms": {
                "Jetpt" : JetHist
            }
        }
        return output
    def postprocess(self):
        pass

Output_class = JetKinem()
Output = Output_class.process(events)

print(Output)
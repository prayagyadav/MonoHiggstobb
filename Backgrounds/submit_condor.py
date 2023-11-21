from monoHbbtools.Utilities import condor 
from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
from coffea import processor
from coffea.analysis_tools import PackedSelection
import awkward as ak
import hist
from matplotlib import pyplot as plt
import mplhep as hep
import logging
import numpy as np

# events = NanoEventsFactory.from_root(
#     "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
#     schemaclass=NanoAODSchema.v7,
#     metadata={"Dataset":"MET_Run2018"}
#     ).events()

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

executor = "futures"

files = {
    "MET_Run2018": {
        "MET_Run2018A": [
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/10C73E73-0C15-2F4B-9E0B-E3DE1C54A597.root",
            "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/1E8B7F5A-4B29-8F46-B2E1-549805E5CBB2.root",
            ]
        }
    }

#For futures execution
if executor == "futures" :
    
    #files = getDataset(keymap=inputs.keymap, mode="divide", files=inputs.files)
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=4),
        schema=NanoAODSchema,
        chunksize= 500000 ,
        maxchunks= None,
        xrootdtimeout=120
    )
    Output = futures_run(
        files,
        "Events",
        processor_instance=JetKinem()
    )

elif executor == "condor" :
    #Create a console log in case of a warning 
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=logging.WARNING,
    )
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=500000,
        maxchunks=None,
        xrootdtimeout=300,
    )
    print("Starting the workers...\n")
    Output = runner(
        files,
        treename="Events",
        processor_instance=JetKinem()
    )

print(Output)
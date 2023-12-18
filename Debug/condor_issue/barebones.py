from coffea import processor
from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
import awkward as ak
import condor 


class barebones(processor.ProcessorABC):
    def __init__(self):
        pass
    def process(self, events):
        output = events.Jet.pt
        return output
    def postprocess(self, accumulator):
        pass


events = NanoEventsFactory.from_root(
    "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root",
    entry_stop=100000,
    schemaclass=NanoAODSchema,
    metadata={"dataset":"MET_Run2018A"}
).events()

p = barebones()
a = barebones.process(events)

print(a)


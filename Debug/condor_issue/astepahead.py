from coffea import processor
from coffea.nanoevents import NanoAODSchema , NanoEventsFactory
import awkward as ak
import condor 

print("Stage 1")

class barebones(processor.ProcessorABC):
    def __init__(self):
        pass
    def process(self, events):
        output = events.Jet.pt
        return output
    def postprocess(self, accumulator):
        pass

print("Stage 2")

filename = "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root"
exec = "futures"
Mode = "MET_Run2018"

print("Stage 3")

#For local execution
if exec == "futures" :
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=1),
        schema=NanoAODSchema,
        chunksize= 100000,
        maxchunks= 1,
    )

    Files = {"MET_Run2018":{"MET_Run2018A":[filename,]}}

    print("Stage 4")

    Output = futures_run(
        Files[Mode],
        "Events",
        processor_instance=barebones()
    )

#For condor execution
elif exec == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=100000,
        maxchunks=2,
        xrootdtimeout=300,
    )

    Files = {"MET_Run2018":{"MET_Run2018A":[filename,]}}

    print("Stage 4")

    print("Running...\n")
    Output = runner(
        Files[Mode],
        treename="Events",
        processor_instance=barebones(),
    )

print("stage 5")

print(Output)

print("Stage 6")
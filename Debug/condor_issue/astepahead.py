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


filename = "root://cmsxrootd.fnal.gov///store/data/Run2018A/MET/NANOAOD/UL2018_MiniAODv2_NanoAODv9-v2/110000/0F8C0C8C-63E4-1D4E-A8DF-506BDB55BD43.root"
exec = "futures"
Mode = "MET_Run2018A"

#For local execution
if exec == "futures" :
    with open("fileset.json") as f: #load the fileset
        files = {"MET_Run2018A": [filename]}
    futures_run = processor.Runner(
        executor = processor.FuturesExecutor(workers=2),
        schema=NanoAODSchema,
        chunksize= 100000,
        maxchunks= 2,
    )
    Output = futures_run(
        files[Mode],
        "Events",
        processor_instance=barebones()
    )

#For condor execution
elif exec == "condor" :
    print("Preparing to run at condor...\n")
    executor , client = condor.runCondor()
    client.upload_file("fileset.json")
    with open("fileset.json") as f:
        files = {"MET_Run2018A": [filename]}

    runner = processor.Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=100000,
        maxchunks=2,
        xrootdtimeout=300,
    )
    print("Running...\n")
    Output = runner(
        files[Mode],
        treename="Events",
        processor_instance=barebones(),
    )


print(Output)

import awkward as ak
import uproot as ur
import hist
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
redirector = "root://xrootd-cms.infn.it//"
file_name = "/store/mc/RunIISummer20UL18NanoAODv9/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/130000/10671CD1-1127-0842-AE7F-AF20DA985C45.root"
events = NanoEventsFactory.from_root(
    redirector+file_name, 
    schemaclass=NanoAODSchema.v7,
    metadata={"Datasets":"TTTo2L2Nu"},
	 entry_stop = 10000,
).events()
print("Number of events: ", len(events))
h = hist.Hist.new.Reg(100,0,200).Double()
h.fill(ak.flatten(events.Electron.pt))
h.plot()
plt.ylabel("Events / 2 GeV")
plt.savefig("TestPlot.jpg", dpi=300)

import awkward as ak
import uproot as ur
import hist
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
redirector = "/afs"
file_name = "/hep.wisc.edu/user/ssatapathy/public_html/RootFiles/7D2FDF02-289C-B14E-A47F-A24891AB6106.root"
events = NanoEventsFactory.from_root(
    redirector+file_name, 
    schemaclass=NanoAODSchema.v7,
    metadata={"Datasets":"TTTo2L2Nu"},
	 #entry_stop = 10000,
).events()
print("Number of events: ", len(events))
h = hist.Hist.new.Reg(100,0,200).Double()
h.fill(ak.flatten(events.Electron.pt))
h.plot()
plt.ylabel("Events / 2 GeV")
plt.savefig("MCTestPlot.jpg", dpi=300)

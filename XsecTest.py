import awkward as ak
from coffea.analysis_tools import PackedSelecction
from coffea.nanoevents import NanoEventsFactory , NanoAODSchema
import hist
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
import uproot

redirector = {
	"hdfs":"/hdfs",
	"global":"root://cmsxrd.global.cern.ch",
	"":
}

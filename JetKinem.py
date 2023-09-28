import uproot
import awkward as ak
from coffea.nanoevents import NanoEventsFactory , NanoAODSchema
import hist
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
events = NanoEventsFactory.from_root(
    "./3BF5BC57-5D24-1C4D-B277-7B0BAA5DF3C7.root",
    schemaclass= NanoAODSchema.v7,
    metadata= {"Dataset":"Single Electrons"}
).events()
Jets = events.Jet[events.Jet.pt > 25]
Tag = hist.Hist.new.Reg(20,-1,1).Double().fill(ak.flatten(Jets.btagCSVV2)).plot()
Tag.plot()
plt.savefig("Tag.png", dpi= 320)
btag_WP_medium = 0.3040
ak4_BJets_med = Jets[ Jets.btagDeepFlavB > btag_WP_medium ]
JetHist= hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type").Reg(100,0.,400., name="pt", label="$p_t$ (GeV)").Double()
JetHist.fill(Type="Untagged", pt = ak.flatten(Jets.pt))
JetHist.fill(Type="btagDeepFlavB", pt = ak.flatten(ak4_BJets_med.pt))
JetHist.plot()
plt.ylabel("Events/4 GeV")
hep.cms.label("Preliminary", data=True)
plt.legend()
plt.savefig("Jets.png", dpi= 320)
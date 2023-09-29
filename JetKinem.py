import uproot
import awkward as ak
from coffea.nanoevents import NanoEventsFactory , NanoAODSchema
import hist
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
plt.style.use(hep.style.CMS)
redirector = "root://cmsxrootd.fnal.gov//"
events = NanoEventsFactory.from_root(
    redirector+"/store/mc/RunIIAutumn18NanoAODv7/MonoHTobb_ZpBaryonic_TuneCP2_13TeV_madgraph-pythia8/NANOAODSIM/Nano02Apr2020_rp_102X_upgrade2018_realistic_v21-v1/10000/0EE0D641-EDAE-D547-ABAD-56D54B768C5B.root",
    schemaclass= NanoAODSchema.v7,
    metadata= {"Dataset":"Single Electrons"}
).events()
Jets = events.Jet[events.Jet.pt > 25]
Tag = hist.Hist.new.Reg(20,-1,1).Double().fill(ak.flatten(Jets.btagCSVV2))
Tag.plot()
plt.savefig("Tag.png", dpi= 320)
plt.clf()
btag_WP_medium = 0.3040
ak4_BJets_med = Jets[ Jets.btagDeepFlavB > btag_WP_medium ]
JetHist= hist.Hist.new.StrCat(["Untagged","btagDeepFlavB"], name="Type").Reg(100,0.,400., name="pt", label="$p_t$ (GeV)").Double()
JetHist.fill(Type="Untagged", pt = ak.flatten(Jets.pt))
JetHist.fill(Type="btagDeepFlavB", pt = ak.flatten(ak4_BJets_med.pt))
JetHist.plot()
plt.ylabel("Events/4 GeV")
hep.cms.label("Preliminary", data=True)
plt.legend()
plt.clf()

#Better Plot
x_min = 0.
x_max = 400.
bin_size = 4
n_bins=int((x_max - x_min)/bin_size)
#h = hist.Hist.new.Reg(n_bins,x_min,x_max).Double().fill(ak.flatten(Jets.pt))
fig , ax= plt.subplots(figsize=(10,10))
hep.histplot(JetHist["Untagged",:], 
             #bins=bins ,
             histtype="fill",
             color="b",
             alpha=0.7,
             edgecolor="black",
             label=r"Untagged",
             ax=ax
            )
hep.histplot(JetHist["btagDeepFlavB",:], 
             #bins=bins ,
             histtype="fill",
             color="r",
             alpha=0.7,
             edgecolor="black",
             label=r"btagDeepFlavB",
             ax=ax
            )
ax.set_title("Jet $p_t$", fontsize=25, color="#053B50")
ax.set_xlabel("$p_t$ (GeV)", fontsize=20)
ax.set_ylabel(f"Events / {bin_size} GeV", fontsize=20)
ax.set_xticks(np.arange(x_min,x_max+bin_size,bin_size))
hep.cms.label("Preliminary",data=False, rlabel="unknown $fb^{-1}$")


ax.legend()
fig.savefig("Jets.png", dpi= 300)




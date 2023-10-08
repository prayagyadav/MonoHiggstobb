"""
This file loads the data obtained from METstudy.py and plots a few kinematics
"""
#################################
# Import the necessary packages #
#################################

import awkward as ak 
from coffea import util
import matplotlib.pyplot as plt
import mplhep as hep
import numpy as np
import os
plt.style.use(hep.style.CMS)

#Load the output
def plot(filename):
    Output = util.load(filename)
    if filename.startswith("Data"):
        Mode = "Data"
    elif filename.startswith("MC"):
        Mode = "MC"
    
    #pt plot
    ptHist = Output["Histograms"]["METpt"]
    fig, ax = plt.subplots()
    hep.histplot(
        [ptHist["noflags",:], ptHist["flags",:] ],
        color=["#525FE1","#F86F03"],
        label=["noflags","flags"],
        lw=2,
        ax=ax
    )
    hep.cms.label("Preliminary", data= Mode == "Data")
    ax.set_ylabel("Events / 5 GeV")
    ax.set_title("MET $p_t$",pad=35)
    fig.legend(loc='upper right', bbox_to_anchor=(0.7, 0.7))
    plotname = f"{Mode}ptMETflags.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

    #phi plot
    phiHist = Output["Histograms"]["METphi"]
    fig, ax = plt.subplots()
    hep.histplot(
        [phiHist["noflags",:], phiHist["flags",:] ],
        color=["#525FE1","#F86F03"],
        label=["noflags","flags"],
        lw=2,
        ax=ax
    )
    hep.cms.label("Preliminary", data= Mode=="Data")
    ax.set_ylabel(f"Events / {round(2*np.pi / 100.0, 3) } radians")
    ax.set_xlabel("$\phi$ (radians)")
    ax.set_title("MET $\phi$",pad=35)
    fig.legend(loc='center', bbox_to_anchor=(0.5, 0.5))
    plotname = f"{Mode}phiMETflags.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

filenames = ["DataMETstudy.coffea", "MCMETstudy.coffea"]
for file in filenames:
    if file in os.listdir():
        plot(file)

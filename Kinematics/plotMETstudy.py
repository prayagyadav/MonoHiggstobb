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
    METhist = Output["Histograms"]["MET"]
    fig, ax = plt.subplots()
    hep.histplot(
        [METhist["noflags",:],METhist["flags",:] ],
        color=["red","green"],
        label=["noflags","flags"],
        lw=1,
        ax=ax
    )
    hep.cms.label("")
    ax.set_ylabel("Events / 5 GeV")
    fig.legend(loc=10)
    plotname = f"{Mode}METflags.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

filenames = ["DataMETstudy.coffea", "MCMETstudy.coffea"]
for file in filenames:
    if file in os.listdir():
        plot(file)
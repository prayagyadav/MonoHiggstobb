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
    METhist = Output["Histograms"]["METhist"]
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
    fig.savefig(f"{Mode}METflags.png", dpi=280)

filenames = ["DataMETflags.png", "MCMETflags.png"]
for file in filenames:
    if file in os.listdir():
        plot(file)
        print(file, "created.")
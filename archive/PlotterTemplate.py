"""
This is a template of the plotter for data obtained from a processor.
Author : Prayag Yadav
Created : 11 October 2023
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
    
    #pt plot
    h = Output["Histograms"]["h"]
    fig, ax = plt.subplots()
    hep.histplot(
        [h["type1",:], h["type2",:] ],
        color=["#525FE1","#F86F03"],
        label=["type1","type2"],
        lw=2,
        ax=ax
    )
    hep.cms.label("Preliminary", data= True)
    ax.set_ylabel("Events")
    ax.set_title("Title",pad=35)
    fig.legend()
    plotname = f"Outputfilename.png"
    fig.savefig(plotname, dpi=300)
    print(plotname , f" created at {os.getcwd()}")

filenames = ["Output.coffea"]
for file in filenames:
    if file in os.listdir():
        plot(file)

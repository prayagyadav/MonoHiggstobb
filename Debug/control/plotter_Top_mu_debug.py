from coffea import util,processor
import os
import rich
import hist
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep
import crossSections


def showinfo(Output):
    for key in Output.keys():
        rich.print(key, " :")
        for subkey in Output[key] :
            rich.print(subkey, " :")
            cutflow = Output[key][subkey]["Cutflow"]
            rich.print(cutflow)
def accum(key):
    path = "coffea_files/debug/"
    list_files = os.listdir(path)
    valid_list = []
    for file in list_files :
        if file.startswith(f"CR_resolved_Top_{key}_from") :
            valid_list.append(file)
    full = processor.accumulate([util.load(path+name) for name in valid_list])
    return full
def recoil_adder(master_dict):
    temp0 = []
    temp1 = {}
    for recoil_window in master_dict.keys():
        temp2 = adder(master_dict[recoil_window])
        for sample in temp2.keys():
            temp1[sample] = temp2[sample]['Cutflow']
        temp0.append(temp1)
    return processor.accumulate(temp0)

def adder(raw_dict , onlydata = False):
    """
    Adds(accumulates) the dictionaries from subcategories using cross section in case of MCs or raw in case of data 
    """
    if onlydata == False:
        raw_dict = xsec_reweight(raw_dict) #Return the same dict with cross section reweighting applied to MCs
    output_dict = {}
    keys = list(raw_dict.keys())
    for key in keys :
        subkeys = list(raw_dict[key].keys())
        temp = []
        for subkey in subkeys :
            temp.append(raw_dict[key][subkey])
        output_dict[key] = processor.accumulate(temp)
    return output_dict

def xsec_reweight(input_dict):
    """
    Returns the same histogram with cross section reweighting applied to every MC histogram object and cutflow dictionary
    """
    lumi = crossSections.lumis[2018]
    #print("Integrated Luminosity(pb): ", lumi)

    #load cross sections

    ZJets_NuNu_xsec = {
        "Z1Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_50To150_18"],
        "Z1Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_150To250_18"],
        "Z1Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_250To400_18"],
        "Z1Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z1Jets_NuNu_ZpT_400Toinf_18"],
        "Z2Jets_NuNu_ZpT_50To150_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_50To150_18"],
        "Z2Jets_NuNu_ZpT_150To250_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_150To250_18"],
        "Z2Jets_NuNu_ZpT_250To400_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_250To400_18"],
        "Z2Jets_NuNu_ZpT_400Toinf_18": crossSections.crossSections["Z2Jets_NuNu_ZpT_400Toinf_18"]
    }

    TTToSemiLeptonic_xsec = {
        "TTToSemiLeptonic_18":crossSections.crossSections["TTToSemiLeptonic_18"]
    }
    
    TTTo2L2Nu_xsec = {
        "TTTo2L2Nu_18": crossSections.crossSections["TTTo2L2Nu_18"]
    }
    
    TTToHadronic_xsec = {
        "TTToHadronic_18": crossSections.crossSections["TTToHadronic_18"]
    }

    WJets_LNu_xsec = {
        "WJets_LNu_WPt_100To250_18": crossSections.crossSections["WJets_LNu_WPt_100To250_18"],
        "WJets_LNu_WPt_250To400_18": crossSections.crossSections["WJets_LNu_WPt_250To400_18"],
        "WJets_LNu_WPt_400To600_18": crossSections.crossSections["WJets_LNu_WPt_400To600_18"],
        "WJets_LNu_WPt_600Toinf_18": crossSections.crossSections["WJets_LNu_WPt_600Toinf_18"],
    }

    DYJets_LL_xsec = {
        "DYJets_LL_Pt_0To50_18": crossSections.crossSections["DYJets_LL_Pt_0To50_18"],
        "DYJets_LL_Pt_50To100_18": crossSections.crossSections["DYJets_LL_Pt_50To100_18"],
        "DYJets_LL_Pt_100To250_18": crossSections.crossSections["DYJets_LL_Pt_100To250_18"],
        "DYJets_LL_Pt_250To400_18": crossSections.crossSections["DYJets_LL_Pt_250To400_18"],
        "DYJets_LL_Pt_400To650_18": crossSections.crossSections["DYJets_LL_Pt_400To650_18"],
        "DYJets_LL_Pt_650Toinf_18": crossSections.crossSections["DYJets_LL_Pt_650Toinf_18"],
    }

    VV_xsec = {
        "WZ_1L1Nu2Q_18": crossSections.crossSections["WZ_1L1Nu2Q_18"],
        "WZ_2L2Q_18": crossSections.crossSections["WZ_2L2Q_18"],
        #"WZ_2Q2Nu_18": crossSections.crossSections["WZ_2Q2Nu_18"],
        "WZ_3L1Nu_18" : crossSections.crossSections["WZ_3L1Nu_18"],
        "ZZ_2L2Nu_18": crossSections.crossSections["ZZ_2L2Nu_18"],
        "ZZ_2L2Q_18": crossSections.crossSections["ZZ_2L2Q_18"],
        "ZZ_2Q2Nu_18": crossSections.crossSections["ZZ_2Q2Nu_18"],
        "ZZ_4L_18": crossSections.crossSections["ZZ_4L_18"],
        "WW_2L2Nu_18": crossSections.crossSections["WW_2L2Nu_18"],
        "WW_1L1Nu2Q_18": crossSections.crossSections["WW_1L1Nu2Q_18"],
    }

    QCD_xsec = {
        #"QCD_HT100To200_18": crossSections.crossSections["QCD_HT100To200_18"],
        "QCD_HT200To300_18": crossSections.crossSections["QCD_HT200To300_18"],
        "QCD_HT300To500_18": crossSections.crossSections["QCD_HT300To500_18"],
        "QCD_HT500To700_18": crossSections.crossSections["QCD_HT500To700_18"],
        "QCD_HT700To1000_18": crossSections.crossSections["QCD_HT700To1000_18"],
        "QCD_HT1000To1500_18": crossSections.crossSections["QCD_HT1000To1500_18"],
        "QCD_HT1500To2000_18": crossSections.crossSections["QCD_HT1500To2000_18"],
        "QCD_HT2000Toinf_18": crossSections.crossSections["QCD_HT2000Toinf_18"],
    }

    ST_xsec = {
        "ST_tchannel_top_18": crossSections.crossSections["ST_tchannel_top_18"],
        "ST_tchannel_antitop_18": crossSections.crossSections["ST_tchannel_antitop_18"],
        "ST_tW_top_18": crossSections.crossSections["ST_tW_top_18"],
        "ST_tW_antitop_18": crossSections.crossSections["ST_tW_antitop_18"]
    }

    #compute weight_factor for ZJets_NuNu 
    N_i = {}
    ZJets_NuNu_weight_factor = {}
    for subkey in ZJets_NuNu_xsec.keys() :
        N_i[subkey] = input_dict["ZJets_NuNu"][subkey]["Cutflow"]["Total events"]
        ZJets_NuNu_weight_factor[subkey] = ( lumi * ZJets_NuNu_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for TTToSemiLeptonic
    N_i = {}
    TTToSemiLeptonic_weight_factor = {}
    for subkey in TTToSemiLeptonic_xsec.keys() :
        N_i[subkey] = input_dict["TTToSemiLeptonic"][subkey]["Cutflow"]["Total events"]
        TTToSemiLeptonic_weight_factor[subkey] = ( lumi * TTToSemiLeptonic_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for TTTo2L2Nu
    N_i = {}
    TTTo2L2Nu_weight_factor = {}
    for subkey in TTTo2L2Nu_xsec.keys() :
        N_i[subkey] = input_dict["TTTo2L2Nu"][subkey]["Cutflow"]["Total events"]
        TTTo2L2Nu_weight_factor[subkey] = ( lumi * TTTo2L2Nu_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for TTToHadronic
    N_i = {}
    TTToHadronic_weight_factor = {}
    for subkey in TTToHadronic_xsec.keys() :
        N_i[subkey] = input_dict["TTToHadronic"][subkey]["Cutflow"]["Total events"]
        TTToHadronic_weight_factor[subkey] = ( lumi * TTToHadronic_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for WJets_LNu
    N_i = {}
    WJets_LNu_weight_factor = {}
    for subkey in WJets_LNu_xsec.keys() :
        N_i[subkey] = input_dict["WJets_LNu"][subkey]["Cutflow"]["Total events"]
        WJets_LNu_weight_factor[subkey] = ( lumi * WJets_LNu_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for DYJets_LL
    N_i = {}
    DYJets_LL_weight_factor = {}
    for subkey in DYJets_LL_xsec.keys() :
        N_i[subkey] = input_dict["DYJets_LL"][subkey]["Cutflow"]["Total events"]
        DYJets_LL_weight_factor[subkey] = ( lumi * DYJets_LL_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for VV
    N_i = {}
    VV_weight_factor = {}
    for subkey in VV_xsec.keys() :
        N_i[subkey] = input_dict["VV"][subkey]["Cutflow"]["Total events"]
        VV_weight_factor[subkey] = ( lumi * VV_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for QCD
    N_i = {}
    QCD_weight_factor = {}
    for subkey in QCD_xsec.keys() :
        N_i[subkey] = input_dict["QCD"][subkey]["Cutflow"]["Total events"]
        QCD_weight_factor[subkey] = ( lumi * QCD_xsec[subkey] )/N_i[subkey]

    #compute weight_factor for ST
    N_i = {}
    ST_weight_factor = {}
    for subkey in ST_xsec.keys() :
        N_i[subkey] = input_dict["ST"][subkey]["Cutflow"]["Total events"]
        ST_weight_factor[subkey] = ( lumi * ST_xsec[subkey] )/N_i[subkey]

    #individually apply cross section
    def hist_xsec(MC_dict, key_weight_factor):
        keys = list(MC_dict.keys())
        
        for key in keys:
            #Reweight the all the cutflow events
            cutflow_keys = list(MC_dict[key]["Cutflow"].keys())
            for cutflow_key in cutflow_keys :
                MC_dict[key]["Cutflow"][cutflow_key] *= key_weight_factor[key]
                
            # Reweight all the histograms
            properties = list(MC_dict[key]["Histograms"].keys())
            for property in properties:
                MC_dict[key]["Histograms"][property] *= key_weight_factor[key]
            #key_hists[key] *= key_weight_factor[key] #legacy
        return MC_dict

    
    input_dict["ZJets_NuNu"] = hist_xsec(input_dict["ZJets_NuNu"] , ZJets_NuNu_weight_factor)
    input_dict["TTToSemiLeptonic"] = hist_xsec(input_dict["TTToSemiLeptonic"], TTToSemiLeptonic_weight_factor)
    input_dict["TTTo2L2Nu"] = hist_xsec(input_dict["TTTo2L2Nu"], TTTo2L2Nu_weight_factor)
    input_dict["TTToHadronic"] = hist_xsec(input_dict["TTToHadronic"], TTToHadronic_weight_factor)
    input_dict["WJets_LNu"] = hist_xsec(input_dict["WJets_LNu"], WJets_LNu_weight_factor)
    input_dict["DYJets_LL"] = hist_xsec(input_dict["DYJets_LL"], DYJets_LL_weight_factor)
    input_dict["VV"] = hist_xsec(input_dict["VV"], VV_weight_factor)
    input_dict["QCD"] = hist_xsec(input_dict["QCD"], QCD_weight_factor)
    input_dict["ST"] = hist_xsec(input_dict["ST"], ST_weight_factor)

    return input_dict


def plot_CR(input_dict,property="dijets_mass"):
    recoil_windows = list(input_dict.keys())
    labels = ["Recoil: [200,250]","Recoil: [250,290]","Recoil: [290,360]","Recoil: [360-420]","Recoil: [420-1000]"]
    
    fig = plt.figure(figsize=(40,10))
    fig.suptitle(f"2018 Resolved Top Mu Control Region: {property}",size=50,color="#192655")
    gs = fig.add_gridspec(2,5,wspace=0,height_ratios=[6,1],hspace=0 )
    axs = gs.subplots(sharex='row',sharey='row' )

    for i in range(len(axs[0,:])):
        axs[0,i].margins(x=0, y = 0.8)
        axs[1,i].margins(x=0, y = 0.8)
        axs[0,i].text(0.3,0.8,labels[i],size=20,transform=axs[0,i].transAxes) #Data label coordinates
        axs[0,i].set_xlabel(" ")
        #axs[0,i].set_xticks([100,110,120,130,140,150])
    axs[0,0].set_ylabel("Events",size=30)
    axs[0,0].set_yscale('log')
    axs[1,0].set_ylim(-1,1)
    axs[1,0].set_ylabel(r'$\frac{Data-Pred}{Pred}$', size=20)
    
    for (i,window) in enumerate(recoil_windows):

        added_dict = adder(input_dict[window])
        
        #Data histogram
        nbins, = added_dict["MET_Run2018"]["Histograms"][property].shape
        counts_data = added_dict["MET_Run2018"]["Histograms"][property].counts()
        (countarray,edges) = added_dict["MET_Run2018"]["Histograms"][property].to_numpy()
        bin_size= (edges[-1]-edges[0]) / (2*nbins)
        hep.histplot(
            added_dict["MET_Run2018"]["Histograms"][property],
            histtype="errorbar",
            yerr=0,
            xerr=bin_size,
            label="Data",
            color="black",
            edges=False,
            ax=axs[0,i]
        )
        hep.histplot(
            (np.full(nbins,0),edges),
            histtype='step',
            color='black',
            linestyle='dotted',
            edges=False,
            ax=axs[1,i]
        )
        
        #MC histogram
        hep.histplot(
            [
                added_dict["ZJets_NuNu"]["Histograms"][property],
                added_dict["QCD"]["Histograms"][property],
                added_dict["VV"]["Histograms"][property],
                added_dict["DYJets_LL"]["Histograms"][property],
                added_dict["WJets_LNu"]["Histograms"][property],
                added_dict["TTToHadronic"]["Histograms"][property],
                added_dict["ST"]["Histograms"][property],
                added_dict["TTTo2L2Nu"]["Histograms"][property],
                added_dict["TTToSemiLeptonic"]["Histograms"][property]
            ],
            histtype="fill",
            color=["deepskyblue","grey","darkseagreen","darkslategrey","cyan","orangered","darkred","palevioletred","red"],
            label=["ZJets_NuNu","QCD","VV","DYJets_LL","WJets_LNu","TTToHadronic","ST","TTTo2L2Nu","TTToSemiLeptonic"],
            edges=False,
            stack=True,
            ax=axs[0,i]
        )

        counts_MC = (added_dict["TTToSemiLeptonic"]["Histograms"][property].counts()+
                     added_dict["ZJets_NuNu"]["Histograms"][property].counts()+
                     added_dict["WJets_LNu"]["Histograms"][property].counts()+
                     added_dict["ST"]["Histograms"][property].counts()+
                     added_dict["VV"]["Histograms"][property].counts()+
                     added_dict["DYJets_LL"]["Histograms"][property].counts()+
                     added_dict["QCD"]["Histograms"][property].counts()+
                     added_dict["TTTo2L2Nu"]["Histograms"][property].counts()+
                     added_dict["TTToHadronic"]["Histograms"][property].counts()
                    )
        ratios = (counts_data-counts_MC)/counts_MC
        # Relative error histogram
        hep.histplot(
            (ratios,edges),
            histtype='errorbar',
            color='black',
            yerr=0,
            xerr=bin_size,
            marker='.',
            edges=False,
            ax=axs[1,i]
        )
    #plt.yscale('log')
    if property == "dijet_mass" :
        plt.xlabel(r"$M_{bb}$ GeV")
        axs[0,0].set_xticks([110,120,130,140,150])
    elif property == "dijets_pt":
        axs[0,0].set_xlim([0.0,500.0])
        plt.xlabel(r"$p_t$ (GeV)")
    elif property == "dijets_eta":
        axs[0,0].set_xlim([-2.5,2.5])
        plt.xlabel(r"$ \eta $")
    elif property == "dijets_phi":
        axs[0,0].set_xlim([-3.14,3.14])
        plt.xlabel(r"$\phi $")
    elif property == "met_pt_hist":
        axs[0,0].set_xlim([0.0,500.0])
        plt.xlabel(r"$p_t$ (GeV)")
    elif property == "met_phi_hist":
        axs[0,0].set_xlim([-3.14,3.14])
        plt.xlabel(r"$\phi $")
    elif ( (property == "leadingjets_mass_hist") | (property == "subleadingjets_mass_hist") ):
        axs[0,0].set_xlim([0.0,500.0])
        plt.xlabel(r"mass (GeV)")
    elif ( (property == "leadingjets_pt_hist") | (property == "subleadingjets_pt_hist") ):
        axs[0,0].set_xlim([0.0,500.0])
        plt.xlabel(r"$p_t$ (GeV)")
    elif ( (property == "leadingjets_eta_hist") | (property == "subleadingjets_eta_hist") ):
        axs[0,0].set_xlim([-2.5,2.5])
        plt.xlabel(r"$ \eta $")
    elif ( (property == "leadingjets_phi_hist") | (property == "subleadingjets_phi_hist") ):
        axs[0,0].set_xlim([-3.14,3.14])
        plt.xlabel(r"$\phi $")
    axs[0,0].legend()
    fig_name=f"CR_resolved_TopMu_{property}.png"
    path="plots/debug/"
    fig.savefig(path+fig_name, dpi=240)

def make_table(keys):
    labels= np.arange(1,len(keys)+1,1)
    fig,ax=plt.subplots()
    # hide axes
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')
    ax.table(np.transpose([labels,keys]),loc='center')
    path="plots/debug/"
    fig.savefig(path+"CR_resolved_TopMu_cutflowinfo_table.png",dpi=240)

def plotCRcutflow(input_dict):
    input_dict = recoil_adder(input_dict)
    
    nbins = len(input_dict['TTToSemiLeptonic'].keys())

    make_table(list(input_dict['TTToSemiLeptonic'].keys()))

    #color=["deepskyblue","grey","darkseagreen","darkslategrey","cyan","orangered","darkred","palevioletred","red"]
    #label=["ZJets_NuNu","QCD","VV","DYJets_LL","WJets_LNu","TTToHadronic","ST","TTTo2L2Nu","TTToSemiLeptonic"]
    hists=[]
    colors=[]
    labels=[]
    for key in input_dict.keys():
        cutflow_dict = input_dict[key]
        histogram = hist.Hist.new.Reg(nbins,1,nbins+1).Double()

        weights = []
        n = 1 
        for item in cutflow_dict.keys():
            if (item.startswith('lumi') | (item == 'MET trigger') | (item == 'MET filters') ):
                continue
            weights.append(cutflow_dict[item])
            #print(n, " : ", item , " : ", cutflow_dict[item])
            n += 1
        #print(f"{key} : ", weights)

        for i in range(nbins):
            histogram.fill(i+1, weight=weights[i])
        #print(histogram)
        if key.startswith('MET_Run'):
            data_hist = histogram
        else :
            hists.append(histogram)

        if key == 'ZJets_NuNu' :
            colors.append('deepskyblue')
            labels.append('ZJets_NuNu')
        elif key == 'QCD' :
            colors.append('grey')
            labels.append('QCD')
        elif key == 'VV' :
            colors.append('darkseagreen')
            labels.append('VV')
        elif key == 'DYJets_LL' :
            colors.append('darkslategrey')
            labels.append('DYJets_LL')
        elif key == 'WJets_LNu' :
            colors.append('cyan')
            labels.append('WJets_LNu')
        elif key == 'TTToHadronic' :
            colors.append('orangered')
            labels.append('TTToHadronic')
        elif key == 'ST' :
            colors.append('darkred')
            labels.append('ST')
        elif key == 'TTTo2L2Nu' :
            colors.append('palevioletred')
            labels.append('TTTo2L2Nu')
        elif key == 'TTToSemiLeptonic' :
            colors.append('red')
            labels.append('TTToSemiLeptonic')
    
    fig, ax = plt.subplots()
    hep.histplot(
        data_hist,
        histtype='errorbar',
        color='black',
        yerr=0,
        xerr=0.5,
        ax=ax
    )
    hep.histplot(
        hists,
        histtype='fill',
        color=colors,
        #marker=[],
        label=labels,
        lw=1,
        stack=True,
        ax=ax
        )

    hep.cms.label("Preliminary", data = False)
    ax.set_ylabel("Events")
    ax.set_xlabel("Cutflow order")
    plt.xlim([1,nbins+1])
    plt.xticks(np.arange(1,nbins+1,1))
    ax.set_title(r"Top $\mu$ cutflow",pad=15,  fontsize= "20", color="#192655")
    fig.legend(prop={"size":8},loc= (0.67,0.54) )
    plt.yscale("log")
    #fig.text(0.01,0.01,"Generated : "+get_timestamp(), fontsize = "10")
    #fig.text(0.87,0.01," Mode: Overlayed", fontsize = "10")
    #fig.legend(loc= (0.70,.91))
    #fig.legend(loc=1)
    plotname = f"CR_resolved_TopMu_cutflow.png"
    path = "plots/debug/"
    fig.savefig(path+plotname, dpi=240)
    #fig.clear()
    print(plotname , f" created at {os.getcwd()}")
    
MET_Run2018 = accum("MET_Run2018")
# TTToSemiLeptonic = accum("TTToSemiLeptonic")
# ZJets_NuNu = accum("ZJets_NuNu")
# WJets_LNu = accum("WJets_LNu")
# ST = accum("ST")
# DYJets_LL = accum("DYJets_LL")
# VV = accum("VV")
# QCD = accum("QCD")
# TTTo2L2Nu = accum("TTTo2L2Nu")
# TTToHadronic = accum("TTToHadronic")
master_dict = processor.accumulate([
    MET_Run2018,
    # ZJets_NuNu,
    # TTToSemiLeptonic,
    # WJets_LNu,
    # TTTo2L2Nu,
    # TTToHadronic,
    # DYJets_LL,
    # VV,
    # QCD,
    # ST
    ])

def overall_cutflow(master_dict,dataset="MET_Run2018"):
    temp1=[]
    temp2=[]
    for key in master_dict:
        added = adder(master_dict[key])
        cat_dict = added[dataset]["Cutflow"]
        print(cat_dict)
        end_slice = {key: cat_dict[key] for key in cat_dict.keys() & ["HEM veto","one_tight_muon","Recoil","At least two bjets","bjet1 pt > 50 GeV ","bjet2 pt > 30 GeV","Additional Jets greater_than_or_equal_to 1","dijet mass between 100 Gev to 150 GeV","dijet pt > 100 GeV"]}
        temp2.append(end_slice) 
        temp1.append(cat_dict)
    front_slice={key: temp1[0][key] for key in temp1[0].keys() & ["Total events","MET trigger","MET filters","MET > 50.0 GeV","no electrons","no photons","no taus"]}
    end_slice=processor.accumulate(temp2)

    merged_dict = end_slice.copy()

    for key, value in front_slice.items():
        merged_dict[key] = value

    return merged_dict

def simpleoverallcutflow(master_dict,dataset="MET_Run2018"):
    temp = []
    nrecoil = 0
    for key in master_dict:
        nrecoil += 1
        added = adder(master_dict[key] , onlydata=True)
        cat_dict = added[dataset]["Cutflow"]
        temp.append(cat_dict)
    output_dict = processor.accumulate(temp)
    #repeated_keys = ["Total events","MET trigger","MET filters","MET > 50.0 GeV","no electrons","no photons","no taus","HEM veto","one_tight_muon"]
    repeated_keys = ["Total events","MET trigger","MET filters","MET > 50.0 GeV","no electrons","no photons","no taus","one_tight_muon"]
    for key in repeated_keys :
        output_dict[key] = output_dict[key] / nrecoil
    #convert values in scientific notation
    output_dict = {key : "{:e}".format(value) for key,value in output_dict.items()}
    return output_dict

def show(dict):
    for key in dict.keys() :
        print(key , " : ", dict[key] , " \n")

show(simpleoverallcutflow(master_dict))

# plot_CR(master_dict,property="dijets_mass")
# plot_CR(master_dict,property="dijets_pt")
# plot_CR(master_dict,property="dijets_eta")
# plot_CR(master_dict,property="dijets_phi")
# plot_CR(master_dict,property="met_pt_hist")
# plot_CR(master_dict,property="met_phi_hist")
# plot_CR(master_dict,property="leadingjets_pt_hist")
# plot_CR(master_dict,property="leadingjets_eta_hist")
# plot_CR(master_dict,property="leadingjets_phi_hist")
# plot_CR(master_dict,property="leadingjets_mass_hist")
# plot_CR(master_dict,property="subleadingjets_pt_hist")
# plot_CR(master_dict,property="subleadingjets_eta_hist")
# plot_CR(master_dict,property="subleadingjets_phi_hist")
# plot_CR(master_dict,property="subleadingjets_mass_hist")
# plotCRcutflow(master_dict)

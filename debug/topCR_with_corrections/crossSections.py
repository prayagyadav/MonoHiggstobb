#########################################################################################################
"""
Obtained this file from Shivani.
Link: https://gitlab.cern.ch/slomte/monohiggstobb/-/blob/master/monoHbb/utils/crossSections.py?ref_type=heads
"""
#########################################################################################################
# Everything in picobarns

hbb_BR = 0.582

lumis = {
    2016: 35860.0,
    2017: 41529.0,
    2018: 58877.0,
}

crossSections = {

    #2017
    #from mono-jet AN:
    "Z1Jets_NuNu_ZpT_50To150_17": 598.9 ,
    "Z1Jets_NuNu_ZpT_150To250_17": 18.04 ,
    "Z1Jets_NuNu_ZpT_250To400_17": 2.051 ,
    "Z1Jets_NuNu_ZpT_400Toinf_17": 0.2251 ,
    "Z2Jets_NuNu_ZpT_50To150_17": 326.3 ,
    "Z2Jets_NuNu_ZpT_150To250_17": 29.6 ,
    "Z2Jets_NuNu_ZpT_250To400_17": 5.174 ,
    "Z2Jets_NuNu_ZpT_400Toinf_17": 0.8472 ,
    "WJets_LNu_WPt_50To100_17": 3569.0,
    "WJets_LNu_WPt_100To250_17": 769.8,
    "WJets_LNu_WPt_250To400_17": 27.62,
    "WJets_LNu_WPt_400To600_17": 3.591,
    "WJets_LNu_WPt_600Toinf_17": 0.549,
    "TTToSemiLeptonic_17": 365.34 ,
    "TTTo2L2Nu_17": 88.29 ,
    "TTToHadronic_17": 377.96 ,
    "ST_tchannel_top_17": 137.458,
    "ST_tchannel_antitop_17": 83.0066,
    "ST_tW_top_17": 35.85,
    "ST_tW_antitop_17": 35.85,

    #from Varun's repo:
    "DYJets_LL_HT_70To100_17": 147.0,
    "DYJets_LL_HT_100To200_17": 161.0,
    "DYJets_LL_HT_200To400_17": 48.58,
    "DYJets_LL_HT_400To600_17": 6.983,
    "DYJets_LL_HT_600To800_17": 1.747,
    "DYJets_LL_HT_800To1200_17": 0.8052,
    "DYJets_LL_HT_1200To2500_17": 0.1927,
    "DYJets_LL_HT_2500ToInf_17": 0.003478,
    "WW_2L2Nu_17": 11.08,
    "WW_1L1Nu2Q_17": 45.99,
    "WW_4Q_17": 47.73,
    #from mono-Higgs AN:
    "WZ_1L1Nu2Q_17": 10.74,
    "WZ_2L2Q_17": 5.60,
    "WZ_2Q2Nu_17": 6.858,#not on DAS
    "WZ_3L1Nu_17" : 4.43,
    "ZZ_2L2Nu_17": 0.56,
    "ZZ_2L2Q_17": 3.22,
    "ZZ_2Q2Nu_17": 4.73,
    "ZZ_4L_17": 1.25,



    #2018
    #from mono-jet AN:
    "Z1Jets_NuNu_ZpT_50To150_18": 598.9 ,
    "Z1Jets_NuNu_ZpT_150To250_18": 18.04 ,
    "Z1Jets_NuNu_ZpT_250To400_18": 2.051 ,
    "Z1Jets_NuNu_ZpT_400Toinf_18": 0.2251 ,
    "Z2Jets_NuNu_ZpT_50To150_18": 326.3 ,
    "Z2Jets_NuNu_ZpT_150To250_18": 29.6 ,
    "Z2Jets_NuNu_ZpT_250To400_18": 5.174 ,
    "Z2Jets_NuNu_ZpT_400Toinf_18": 0.8472 ,
    "WJets_LNu_WPt_50To100_18": 3569.0,
    "WJets_LNu_WPt_100To250_18": 769.8,
    "WJets_LNu_WPt_250To400_18": 27.62,
    "WJets_LNu_WPt_400To600_18": 3.591,
    "WJets_LNu_WPt_600Toinf_18": 0.549,
    "TTToSemiLeptonic_18": 365.34 ,
    "TTTo2L2Nu_18": 88.29 ,
    "TTToHadronic_18": 377.96 ,
    "ST_tchannel_top_18": 137.458,
    "ST_tchannel_antitop_18": 83.0066,
    "ST_tW_top_18": 35.85,
    "ST_tW_antitop_18": 35.85,
    #
    "WZ_1L1Nu2Q_18": 10.74,
    "WZ_2L2Q_18": 5.60,
    "WZ_2Q2Nu_18": 6.858,
    "WZ_3L1Nu_18" : 4.43,
    "ZZ_2L2Nu_18": 0.56,
    "ZZ_2L2Q_18": 3.22,
    "ZZ_2Q2Nu_18": 4.73,
    "ZZ_4L_18": 1.25,
    "WW_2L2Nu_18": 12.18,
    "WW_1L1Nu2Q_18": 50.00,
    "QCD_HT100To200_18": 23690000.0,
    "QCD_HT200To300_18": 1554000.0,
    "QCD_HT300To500_18": 324300.0,
    "QCD_HT500To700_18": 29990.0,
    "QCD_HT700To1000_18": 6374.0,
    "QCD_HT1000To1500_18": 1095.0,
    "QCD_HT1500To2000_18": 99.27,
    "QCD_HT2000Toinf_18": 20.25,

    #from Varun's repo:
    "DYJets_LL_HT_70To100_18": 146.7,
    "DYJets_LL_HT_100To200_18": 160.8,
    "DYJets_LL_HT_200To400_18": 48.63,
    "DYJets_LL_HT_400To600_18": 6.978,
    "DYJets_LL_HT_600To800_18": 1.756,
    "DYJets_LL_HT_800To1200_18": 0.8094,
    "DYJets_LL_HT_1200To2500_18": 0.1931,
    "DYJets_LL_HT_2500ToInf_18": 0.003516,

    #from DAS* fixme
    "DYJets_LL_Pt_0To50_18": 1485.0,
    "DYJets_LL_Pt_50To100_18": 397.4,
    "DYJets_LL_Pt_100To250_18": 97.2,
    "DYJets_LL_Pt_250To400_18": 3.701,
    "DYJets_LL_Pt_400To650_18": 0.5086,
    "DYJets_LL_Pt_650Toinf_18": 0.04728,

    # Xsec values for (MZprime_MChi): 
    #0.05066(1500_100), #0.04976(1500_200), #0.01413(2000_200), #0.2077(1000_100), #3.322 ,
    "MonoHTobb_ZpBaryonic_18": (0.582*0.01413), 
    "MonoHTobb_ZpBaryonic_17": (0.582*0.2077),

}

#These datasets not available on das: 
'''
    "W1Jets_LNu_WpT_100To150_17": 284.06 ,
    "W1Jets_LNu_WpT_150To250_17": 71.71 ,
    "W1Jets_LNu_WpT_250To400_17": 8.06 ,
    "W1Jets_LNu_WpT_400Toinf_17": 0.89 ,
    "W2Jets_LNu_WpT_100To150_17": 281.63 ,
    "W2Jets_LNu_WpT_150To250_17": 108.59 ,
    "W2Jets_LNu_WpT_250To400_17": 18.03 ,
    "W2Jets_LNu_WpT_400Toinf_17": 3.0 ,
    "WJets_LNu_WPt_50To100_17": 3570.0,
    "WJets_LNu_WPt_100To250_17": 779.1,
    "WJets_LNu_WPt_250To400_17": 27.98,
    "WJets_LNu_WPt_400To600_17": 3.604,
    "WJets_LNu_WPt_600Toinf_17": 0.5545,
'''


'''incorrect xsec (from ANv6)
    "DYJets_LL_Pt_0To50_18": 106300.0,
    "DYJets_LL_Pt_50To100_18": 0.5164,
    "DYJets_LL_Pt_100To250_18": 407.9,
    "DYJets_LL_Pt_250To400_18": 96.8,
    "DYJets_LL_Pt_400To650_18": 3.774,
    "DYJets_LL_Pt_650Toinf_18": 0.04796,

    "WJets_LNu_WPt_50To100_18": 3570.0,
    "WJets_LNu_WPt_100To250_18": 779.1,
    "WJets_LNu_WPt_250To400_18": 27.98,
    "WJets_LNu_WPt_400To600_18": 3.604,
    "WJets_LNu_WPt_600Toinf_18": 0.5545,


from DAS: 
    "DYJets_LL_Pt_0To50_18": 1485.,
    "DYJets_LL_Pt_50To100_18": 397.4,
    "DYJets_LL_Pt_100To250_18": 97.2,
    "DYJets_LL_Pt_250To400_18": 3.701,
    "DYJets_LL_Pt_400To650_18": 0.5086,
    "DYJets_LL_Pt_650Toinf_18": 0.04728,

    "QCD_HT200To300_18": 111700.0,
    "QCD_HT300To500_18": 27960.0,
    "QCD_HT500To700_18": 3078.0,
    "QCD_HT700To1000_18": 721.8,
    "QCD_HT1000To1500_18": 138.2,
    "QCD_HT1500To2000_18": 13.61,
    "QCD_HT2000Toinf_18": 2.92,

'''

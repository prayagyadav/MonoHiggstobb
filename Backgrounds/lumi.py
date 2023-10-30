from monoHbbtools.Utilities import lumicalc
import json

with open("../CurrentRunFileset.json") as file :
    runs = json.load(file)
    lumicalc.Generate_Luminosity_file(runs)
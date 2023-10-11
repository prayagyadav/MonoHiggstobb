import json
import rich
import numba

class Loadfileset():
    def __init__(self, jsonfilename) :
        with open(jsonfilename) as f :
            self.handler = json.load(f)

    
    def Show(self , verbosity=1):
        match verbosity :
            case 1 :
                for key, value in self.handler.items() :
                    rich.print(key+" : ", list(value.keys()))
            case 2 :
                for key, value in self.handler.items() :
                    rich.print(key+" : ", list(value.keys()), "\n")
                    for subkey , subvalue in value.items() :
                        rich.print("\t"+subkey+" : ")
                        for file in subvalue :
                            rich.print("\t", file)
    
    @numba.jit(forceobj=True)
    def getFileset(self, mode , key, redirector ) :
        # Construct with desired redirector
        match redirector :
            case "fnal" | 1 :
                redirector_string = "root://cmsxrootd.fnal.gov//"
            case "infn" | 2 :
                redirector_string = "root://xrootd-cms.infn.it//"
            case "wisc" | 3 :
                redirector_string = "root://pubxrootd.hep.wisc.edu//"

        raw_fileset = self.handler[mode][key] 
        requested_fileset = {key : [redirector_string+filename for filename in raw_fileset]}
        return requested_fileset



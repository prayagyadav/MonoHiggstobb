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
            case 3 :
                for key, value in self.handler.items() :
                    rich.print(key+" : ", list(value.keys()), "\n")
                    for subkey , subvalue in value.items() :
                        rich.print("\t"+subkey+" : ")
                        for subsubkey, subsubvalue in subvalue.items() :
                            try :
                                for file in subvalue :
                                    rich.print("\t", file)
                            except:
                                rich.print("\t"+subsubkey+" : ")
    
    @numba.jit(forceobj=True)
    def getFileset(self, mode ,superkey, key, redirector ) :
        # Construct with desired redirector
        match redirector :
            case "fnal" | 1 :
                redirector_string = "root://cmsxrootd.fnal.gov//"
            case "infn" | 2 :
                redirector_string = "root://xrootd-cms.infn.it//"
            case "wisc" | 3 :
                redirector_string = "root://pubxrootd.hep.wisc.edu//"

        raw_fileset = self.handler[mode][superkey][key] 
        requested_fileset = {superkey : [redirector_string+filename for filename in raw_fileset]}
        return requested_fileset
    
    def getraw(self):
        #load the raw dictionary
        full_fileset = self.handler
        return full_fileset


def buildFileset(dict , redirector):
    '''
    To return a run-able dict with the appropriate redirector.
    Please input a dictionary which is only singly-nested
    '''
    redirectors = {
        "fnal": "root://cmsxrootd.fnal.gov//",
        "infn": "root://xrootd-cms.infn.it//",
        "wisc": "root://pubxrootd.hep.wisc.edu//"

    }
    match redirector :
        case "fnal" | 1 :
            redirector_string = redirectors["fnal"]
        case "infn" | 2 :
            redirector_string = redirectors["infn"]
        case "wisc" | 3 :
            redirector_string = redirectors["wisc"]

    temp = dict 
    output = {}
    for key in temp.keys() :
        try :
            g = temp[key]
            if isinstance(g,list):
                templist = []
                for filename in g :
                    filename = filename[filename.find("/store/") :]
                    templist.append(redirector_string+filename)
                output[key] = templist
        except :
            raise KeyError
    return output

def make_fileset(fileset_dictionary, Redirector="fnal"):
    """
    Returns the a triple nested fileset with desired redirector
    """
    output = {}
    for Mode in fileset_dictionary.keys() :
        output[Mode] = {}
        for key in fileset_dictionary[Mode] :
            output[Mode][key] = buildFileset(fileset_dictionary[Mode][key], redirector=Redirector)
    return output
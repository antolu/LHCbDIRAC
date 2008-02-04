from DIRAC.Core.Subprocess import shellCall

class copyXMLfile:
    def __init__(self):
        pass
        
    def copyXML(self, fileName):
        """
        copy file from volhcb07 to volhcb05(temporary code)
        """
        cmd = "scp %s zmathe@volhcb05:/storage/XMLProcessing/ToDo/"%(filename)
        result = shellCall(20, cmd)
        return result
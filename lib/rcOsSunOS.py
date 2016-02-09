import rcOs
from rcUtilities import justcall

class Os(rcOs.Os):
    def reboot(self):
        justcall(['reboot', '-q'])

    def crash(self):
        justcall(['halt', '-q'])

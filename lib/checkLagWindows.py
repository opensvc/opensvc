import checks
import os
import wmi

class check(checks.check):
    chk_type = "lag"
    chk_name = "Windows network link aggregate"

    def do_check(self):
        try:
            self.w = wmi.WMI(namespace="root\hpq")
        except:
            # no HP lag
            return []
        r = []
        for team in self.w.HP_EthernetTeam():
            r += self.do_check_team(team)
        return r

    def do_check_team(self, team):
        r = []
        inst = team.Description
        val = team.RedundancyStatus
        r.append({
                'chk_instance': inst+'.redundancy',
                'chk_value': str(val),
                'chk_svcname': '',
               })
        return r

if __name__ == "__main__":
    o = check()
    o.do_check()

import foreign.wmi as wmi

import drivers.check

class Check(drivers.check.Check):
    chk_type = "lag"
    chk_name = "Windows network link aggregate"

    def do_check(self):
        try:
            self.w = wmi.WMI(namespace=r"root\hpq")
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
                "instance": inst+'.redundancy',
                "value": str(val),
                "path": '',
               })
        return r

if __name__ == "__main__":
    o = Check()
    o.do_check()

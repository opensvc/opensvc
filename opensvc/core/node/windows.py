from __future__ import print_function

import os
import time

import core.exceptions as ex

from .node import Node as BaseNode

try:
    import pythoncom
    import foreign.wmi as wmi
    import win32serviceutil
    from foreign.six.moves import winreg
except ImportError:
    raise


WINSVCNAME = "OsvcAgent"

class Node(BaseNode):
    def shutdown(self):
        cmd = ["shutdown", "/s", "/f"]
        ret, out, err = self.vcall(cmd)

    def _reboot(self):
        cmd = ["shutdown", "/r", "/f"]
        ret, out, err = self.vcall(cmd)

    def wmi(self):
        pythoncom.CoInitialize()
        return wmi.WMI()

    def stats_meminfo(self):
        """
        Memory sizes are store in MB.
        Avails are percentages.
        """
        raw_data = {}
        data = {}
        wmi = self.wmi()
        queueinfo = wmi.Win32_PerfFormattedData_PerfOS_System()
        swapinfo = wmi.Win32_PageFileUsage()
        meminfo = wmi.Win32_ComputerSystem()
        perfinfo = wmi.Win32_PerfRawData_PerfOS_Memory()
        raw_data["queuelength"] = int(queueinfo[-1].ProcessorQueueLength)
        raw_data["SwapAvailable"] = int(swapinfo[-1].AllocatedBaseSize) - int(swapinfo[-1].CurrentUsage)
        raw_data["SwapTotal"] = int(swapinfo[-1].AllocatedBaseSize)
        raw_data["MemAvailable"] = int(perfinfo[-1].AvailableBytes)
        raw_data["MemTotal"] = int(meminfo[-1].TotalPhysicalMemory)
        data["mem_total"] = raw_data["MemTotal"] // 1048576
        data["mem_avail"] = int(100 * raw_data["MemAvailable"] // raw_data["MemTotal"])
        data["swap_total"] = raw_data["SwapTotal"]
        data["swap_avail"] = int(100 * raw_data["SwapAvailable"] // raw_data["SwapTotal"])
        data["load_15m"] = raw_data["queuelength"]
        return data

    def daemon_start_native(self):
        self.set_upgrade_envvar()
        try:
            win32serviceutil.StartService(WINSVCNAME)
        except Exception as exc:
            raise ex.Error(str(exc))
        finally:
            self.unset_upgrade_envvar()

        def fn():
            _, state, _, _, _, _, _ = win32serviceutil.QueryServiceStatus(WINSVCNAME)
            if state == 4:
                return True
            return False

        for step in range(5):
            if fn():
                return
            time.sleep(1)
        raise ex.Error("waited too long for startup")

    def daemon_stop_native(self):
        try:
            win32serviceutil.StopService(WINSVCNAME)
        except Exception as exc:
            raise ex.Error(str(exc))

    def unset_upgrade_envvar(self):
        path = r"SYSTEM\CurrentControlSet\Services\%s\Environment" % WINSVCNAME
        reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
        try:
            key = winreg.OpenKey(reg, path, 0, winreg.KEY_WRITE)
            winreg.DeleteValue(key, "OPENSVC_AGENT_UPGRADE")
            winreg.CloseKey(key)
        except Exception as exc:
            if hasattr(exc, "errno") and getattr(exc, "errno") == 2:
                # key does not exist
                return
            raise
        finally:
            winreg.CloseKey(reg)

    def set_upgrade_envvar(self):
        if not os.environ.get("OPENSVC_AGENT_UPGRADE"):
            return
        path = r"SYSTEM\CurrentControlSet\Services\%s\Environment" % WINSVCNAME
        reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
        key = winreg.CreateKeyEx(reg, path, 0, winreg.KEY_WRITE)
        try:
            winreg.SetValue(key, "OPENSVC_AGENT_UPGRADE", winreg.REG_SZ, "1")
        except EnvironmentError:
            raise ex.Error("failed to set OPENSVC_AGENT_UPGRADE=1 in %s" % path)
        winreg.CloseKey(key)
        winreg.CloseKey(reg)

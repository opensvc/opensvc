import time

try:
    import pythoncom
    import wmi
    import win32serviceutil
except ImportError:
    raise

import node
import rcExceptions as ex

WINSVCNAME = "OsvcAgent"

class Node(node.Node):
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
        try:
            win32serviceutil.StartService(WINSVCNAME)
        except Exception as exc:
            raise ex.excError(str(exc))

        def fn():
            _, state, _, _, _, _, _ = win32serviceutil.QueryServiceStatus(WINSVCNAME)
            if state == 4:
                return True
            return False

        for step in range(5):
            if fn():
                return
            time.sleep(1)
        raise ex.excError("waited too long for startup")

    def daemon_stop_native(self):
        try:
            win32serviceutil.StopService(WINSVCNAME)
        except Exception as exc:
            raise ex.excError(str(exc))

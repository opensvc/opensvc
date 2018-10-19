# pylint: skip-file

'''
    Windows Stats - (C) 2013-2017 Mike Miller, fix: Qian Deng
    A simple pip-able Windows status retrieval module with no additional
    dependencies.

    License:
        LGPL, Version 3 (or later)
'''
import ctypes, string
from ctypes import (Structure, Union, WinError, byref, c_double, c_longlong,
                    c_ulong, c_ulonglong, c_size_t, sizeof)
from ctypes.wintypes import HANDLE, LONG, LPCSTR, LPCWSTR, DWORD
from collections import namedtuple

__version__      = '0.70'
LPDWORD = PDWORD = ctypes.POINTER(DWORD)

try:
    unicode
except NameError:
    unicode = str


# Mem Stats-------------------------------------------------------------------
kernel32 = ctypes.windll.kernel32

class MemoryStatusEX(Structure):
    ''' I/O Struct for Windows .GlobalMemoryStatusEx() call.

        Docs:
            http://msdn.microsoft.com/en-us/library/windows/desktop/aa366770
    '''
    _fields_ = [
        ('Length', c_ulong),
        ('MemoryLoad', c_ulong),
        ('TotalPhys', c_ulonglong),
        ('AvailPhys', c_ulonglong),
        ('TotalPageFile', c_ulonglong),
        ('AvailPageFile', c_ulonglong),
        ('TotalVirtual', c_ulonglong),
        ('AvailVirtual', c_ulonglong),
        ('AvailExtendedVirtual', c_ulonglong),
    ]
    def __init__(self):
        # have to initialize this to the size of MemoryStatusEX
        self.Length = sizeof(self)
        super(MemoryStatusEX, self).__init__()


def get_mem_info():
    ''' Returns a Windows Memory Status info object.

        Docs:
            http://msdn.microsoft.com/en-us/library/windows/desktop/aa366589
    '''
    meminfo = MemoryStatusEX()
    kernel32.GlobalMemoryStatusEx(byref(meminfo))
    return meminfo


# Perf Stats -----------------------------------------------------------------
psapi = ctypes.windll.psapi

class PerformanceInfo(Structure):
    ''' I/O struct for Windows .GetPerformanceInfo() call.

        Docs:
            http://msdn.microsoft.com/en-us/library/ms684824
    '''
    _fields_ = [
        ('size',        c_ulong),
        ('CommitTotal', c_size_t),
        ('CommitLimit', c_size_t),
        ('CommitPeak', c_size_t),
        ('PhysicalTotal', c_size_t),
        ('PhysicalAvailable', c_size_t),
        ('SystemCache', c_size_t),
        ('KernelTotal', c_size_t),
        ('KernelPaged', c_size_t),
        ('KernelNonpaged', c_size_t),
        ('PageSize', c_size_t),
        ('HandleCount', c_ulong),
        ('ProcessCount', c_ulong),
        ('ThreadCount', c_ulong),
    ]
    def __init__(self):
        self.size = sizeof(self)
        super(PerformanceInfo, self).__init__()


def get_perf_info():
    ''' Returns a Windows Performance info object.

        Docs:
            http://msdn.microsoft.com/en-us/library/ms683210
        Note:
            Has an extra convenience member: SystemCacheBytes
    '''
    pinfo = PerformanceInfo()
    psapi.GetPerformanceInfo(byref(pinfo), pinfo.size)
    pinfo.SystemCacheBytes = (pinfo.SystemCache * pinfo.PageSize)
    return pinfo


# Disk Stats -----------------------------------------------------------------
_diskusage = namedtuple('disk_usage', 'total used free')

def get_fs_usage(drive):
    ''' Return stats for the given drive.

        Arguments:
            drive       Drive letter.
        Returns:
            A named tuple with total, used, and free members.
        Raises:
            ctypes.WinError
        Recipe:
            http://code.activestate.com/recipes/577972-disk-usage/
    '''
    if len(drive) < 3:
        drive = drive + ':\\'
    _, total, free = c_ulonglong(), c_ulonglong(), c_ulonglong()
    if isinstance(drive, unicode):
        fun = kernel32.GetDiskFreeSpaceExW
    else:
        fun = kernel32.GetDiskFreeSpaceExA

    ret = fun(str(drive), byref(_), byref(total), byref(free))
    if ret == 0:
        raise WinError()
    used = total.value - free.value

    return _diskusage(total.value, used, free.value)


def get_drives():
    ''' Return a list of current drive letters.
        Recipe:
            http://stackoverflow.com/a/827398/450917
    '''
    drives = []
    bitmask = kernel32.GetLogicalDrives()
    try:
        letters = string.uppercase
    except AttributeError as err:  # Py3
        letters = string.ascii_uppercase

    for letter in letters:
        if bitmask & 1:
            drives.append(letter)
        bitmask >>= 1

    return drives


_drive_types = {
    0: 'UNKNOWN',
    1: 'NO_ROOT_DIR',
    2: 'REMOVABLE',
    3: 'FIXED',
    4: 'REMOTE',
    5: 'CDROM',
    6: 'RAMDISK',
}

def get_drive_type(drive):
    ''' Return the type of the given drive, such as
            Fixed, Remote, Optical, etc as a string.

        Docs:
            http://msdn.microsoft.com/en-us/library/windows/desktop/aa364939
    '''
    result = kernel32.GetDriveTypeA(drive)
    return result, _drive_types.get(result, 'UNKNOWN')


_volinfo = namedtuple('vol_info', 'name fstype serialno length flags')

def get_vol_info(drive):
    ''' Retrieve Volume Info for the given drive.

        Docs:
            http://msdn.microsoft.com/en-us/library/windows/desktop/aa364993
        Returns:
            A named tuple containing name and fstype members.
        Note:
            Could use some improvement, such as implementing filesystem flags.
        Recipe:
            http://stackoverflow.com/a/12056414/450917
    '''
    if len(drive) < 3:
        drive = drive + ':\\'
    drive = unicode(drive)
    nameBuf = ctypes.create_unicode_buffer(1024)
    fsTypeBuf = ctypes.create_unicode_buffer(1024)
    serialno = LPDWORD()
    max_component_length = None
    file_system_flags = None

    kernel32.GetVolumeInformationW(
        ctypes.c_wchar_p(drive),
        nameBuf,
        sizeof(nameBuf),
        serialno,
        max_component_length,
        file_system_flags,
        fsTypeBuf,
        sizeof(fsTypeBuf)
    )
    try:                serialno = serialno.contents  # NULL pointer access
    except ValueError:  serialno = None               # not sure what to do
    return _volinfo(nameBuf.value, fsTypeBuf.value, serialno, None, None)


# PerfMon --------------------------------------------------------------------
HQUERY = HCOUNTER = HANDLE
pdh             = ctypes.windll.pdh
# http://msdn.microsoft.com/en-us/library/windows/desktop/aa372637
PDH_FMT_RAW     = 16  # L
PDH_FMT_ANSI    = 32  # L
PDH_FMT_UNICODE = 64  # L
PDH_FMT_LONG    = 256  # L
PDH_FMT_DOUBLE  = 512  # L
PDH_FMT_LARGE   = 1024  # L
PDH_FMT_1000    = 8192  # L
PDH_FMT_NODATA  = 16384  # L
PDH_FMT_NOSCALE = 4096  # L

# http://msdn.microsoft.com/en-us/library/aa373046
_pdh_errcodes = {
    0x00000000: 'PDH_CSTATUS_VALID_DATA',
    0x800007d0: 'PDH_CSTATUS_NO_MACHINE',
    0x800007d2: 'PDH_MORE_DATA',
    0x800007d5: 'PDH_NO_DATA',
    0xc0000bb8: 'PDH_CSTATUS_NO_OBJECT',
    0xc0000bb9: 'PDH_CSTATUS_NO_COUNTER',
    0xc0000bbb: 'PDH_MEMORY_ALLOCATION_FAILURE',
    0xc0000bbc: 'PDH_INVALID_HANDLE',
    0xc0000bbd: 'PDH_INVALID_ARGUMENT',
    0xc0000bc0: 'PDH_CSTATUS_BAD_COUNTERNAME',
    0xc0000bc2: 'PDH_INSUFFICIENT_BUFFER',
    0xc0000bc6: 'PDH_INVALID_DATA',
    0xc0000bd3: 'PDH_NOT_IMPLEMENTED',
    0xc0000bd4: 'PDH_STRING_NOT_FOUND',
}

class PDH_Counter_Union(Union):
    'http://msdn.microsoft.com/en-us/library/windows/desktop/aa373050'
    _fields_ = [
        ('longValue', LONG),
        ('doubleValue', c_double),
        ('largeValue', c_longlong),
        ('ansiValue', LPCSTR),              # aka AnsiString...
        ('unicodeValue', LPCWSTR)           # aka WideString..
    ]

class PDHFmtCounterValue(Structure):
    'http://msdn.microsoft.com/en-us/library/aa373050'
    _fields_ = [
        ('CStatus', DWORD),
        ('union', PDH_Counter_Union),
    ]

def get_pd_err(code):
    'Convert a PDH error code to a human readable string.'
    code &= 2 ** 32 - 1  # signed to unsigned :/
    return _pdh_errcodes.get(code, code)


getfmt = lambda fmt: globals().get('PDH_FMT_' + fmt.upper(), PDH_FMT_LONG)

def get_perf_data(counters, fmts='long', english=False, delay=0):
    ''' Wrap up PerfMon's low-level API.

        Arguments:
            counters        Localized Windows PerfMon counter name, or list of.
            fmts            One of 'long', 'double', 'large', 'ansi', 'unicode'
                            If a list, must match the length of counters.
            english         Add locale-neutral counters in English.
            delay           Some metrics need a second attempt after a delay
                            to acquire (as int ms).
        Returns:
            Tuple of requested counter value(s).
        Raises:
            WindowsError
        Recipes:
            http://msdn.microsoft.com/en-us/library/windows/desktop/aa373214
            http://code.activestate.com/recipes/
                576631-get-cpu-usage-by-using-ctypes-win32-platform/
    '''
    if type(counters) is list:  counters = [ unicode(c)  for c in counters ]
    else:                       counters = [ unicode(counters) ]
    if type(fmts) is list:
        ifmts = [ getfmt(fmt)  for fmt in fmts ]
    else:
        ifmts = [getfmt(fmts)]
        fmts  = [fmts]
    if english:  addfunc = pdh.PdhAddEnglishCounterW
    else:        addfunc = pdh.PdhAddCounterW
    hQuery = HQUERY()
    hCounters = []
    values = []

    # Open Sie, bitte
    errs = pdh.PdhOpenQueryW(None, 0, byref(hQuery))
    if errs:
        pdh.PdhCloseQuery(hQuery)
        raise WindowsError('PdhOpenQueryW failed: %s' % get_pd_err(errs))

    # Add Counter
    for counter in counters:
        hCounter = HCOUNTER()
        errs = addfunc(hQuery, counter, 0, byref(hCounter))
        if errs:
            pdh.PdhCloseQuery(hQuery)
            raise WindowsError('PdhAddCounterW failed: %s' % get_pd_err(errs))
        hCounters.append(hCounter)

    # Collect
    errs = pdh.PdhCollectQueryData(hQuery)
    if errs:
        pdh.PdhCloseQuery(hQuery)
        raise WindowsError('PdhCollectQueryData failed: %s' % get_pd_err(errs))
    if delay:
        kernel32.Sleep(delay)
        errs = pdh.PdhCollectQueryData(hQuery)
        if errs:
            pdh.PdhCloseQuery(hQuery)
            raise WindowsError('PdhCollectQueryData failed: %s' %
                                 get_pd_err(errs))

    # Format
    for i, hCounter in enumerate(hCounters):
        value = PDHFmtCounterValue()
        errs = pdh.PdhGetFormattedCounterValue(hCounter, ifmts[i], None,
                                               byref(value))
        if errs:
            pdh.PdhCloseQuery(hQuery)
            raise WindowsError(('PdhGetFormattedCounterValue failed: %s' %
                                 get_pd_err(errs)))
        values.append(value)

    # Close
    errs = pdh.PdhCloseQuery(hQuery)
    if errs:
        raise WindowsError('PdhCloseQuery failed: %s' % get_pd_err(errs))

    return tuple([ getattr(value.union, fmts[i] + 'Value')  # tuple makes it
                   for i, value in enumerate(values) ])     # possible to use %


# ----------------------------------------------------------------------------

if __name__ == '__main__':

    import locale
    locale.setlocale(locale.LC_ALL, '')
    fmt = lambda x: locale.format('%d', x, True)

    print('Memory Stats:')
    meminfo = get_mem_info()
    print('    Total: %s b' % fmt(meminfo.TotalPhys))
    print('    usage: %s%%' % fmt(meminfo.MemoryLoad))
    print()

    print('Performance Stats:')
    pinfo = get_perf_info()
    print('    Cache: %s p' % fmt(pinfo.SystemCache,))
    print('    Cache: %s b' % fmt(pinfo.SystemCacheBytes))
    print()

    print('Disk Stats:')
    drives = get_drives()
    drive = drives[0]
    print('    Disks:', ', '.join(drives))
    fsinfo = get_fs_usage('%s:\\' % drive)
    vinfo = get_vol_info(drive)
    print('    %s:\\' % drive)
    print('        Name:', vinfo.name, vinfo.serialno)
    print('        Type:', vinfo.fstype)
    print('        Total:', fmt(fsinfo.total))
    print('        Used: ', fmt(fsinfo.used))
    print('        Free: ', fmt(fsinfo.free))
    print()

    print('PerfMon queries:')
    # take a second snapshot 100ms after the first:
    usage = get_perf_data(r'\Processor(_Total)\% Processor Time',
                                   fmts='double', delay=100)
    print('    CPU Usage: %.02f %%' % usage)

    # query multiple at once:
    counters = [ r'\Paging File(_Total)\% Usage', r'\Memory\Available MBytes']
    results = get_perf_data(counters, fmts='double large'.split())
    print('    Pagefile Usage: %.2f %%, Mem Avail: %s MB' % results)


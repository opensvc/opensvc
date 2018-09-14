import os
import glob

def systemd_escape(s):
    def escape(s):
        return "".join([c if c.isalnum() else "\\x%02x"%ord(c) for c in s])
    return "-".join([escape(seg) for seg in s.split("/")])

def systemd_unescape(s):
    return "/".join([seg.encode("utf8").decode("unicode_escape") for seg in s.split("-")])

def systemd_system():
    try:
        return "systemd" in os.readlink("/proc/1/exe")
    except:
        return False

def systemd_get_procs(unit):
    path = glob.glob("/sys/fs/cgroup/unified/system.slice/%s/cgroup.procs" % unit)
    if path:
        return path[0]
    path = glob.glob("/sys/fs/cgroup/systemd/system.slice/%s/tasks" % unit)
    if path:
        return path[0]

def systemd_join(unit):
    path = systemd_get_procs(unit)
    if not path:
        return
    with open(path, "w") as ofile:
        ofile.write(str(os.getpid()))

if __name__ == "__main__":
    s = "opensvc/foo-bar#1"
    print(s)
    s = systemd_escape(s)
    print(s)
    print(systemd_unescape(s))

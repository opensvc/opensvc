"""
Defines the resource set class, which is a collection of resources.
"""
from __future__ import print_function

import os
import sys
import logging

import exceptions as ex
import rcStatus
from rcUtilities import lazy
from rcGlobalEnv import rcEnv
from resources import Resource

class ResourceSet(object):
    """
    Define a set of resources of the same type.
    Example: ResourceSet("fs", [m1, m2])
    """
    def __init__(self,
                 rid=None,
                 resources=None,
                 parallel=False,
                 optional=False,
                 disabled=False,
                 tags=None):
        self.parallel = parallel
        self.svc = None
        self.rid = rid
        self.optional = optional
        self.disabled = disabled
        self.tags = tags
        self.resources = []
        if resources is not None:
            for resource in resources:
                self += resource
        try:
            self.driver_group = self.rid.split(":")[0]
        except ValueError:
            self.driver_group = ""

    def __lt__(self, other):
        return self.rid < other.rid

    def __iadd__(self, other):
        """
        Add a resource to the resourceset.

        Example 1: iadd another ResourceSet: R+=ResSet ... R+=[m1,m2]
        Example 2: iadd a single Resource: R+=ip1
        """
        if isinstance(other, ResourceSet):
            self.resources.extend(other.resources)
        elif isinstance(other, Resource):
            # setup a back pointer to the resource set
            other.rset = self
            self.resources.append(other)
            if hasattr(other, 'sort_rset'):
                other.sort_rset(self)
        return self

    def __str__(self):
        output = "resSet %s [" % str(self.rid)
        for resource in self.resources:
            output += " (%s)" % (resource.__str__())
        return "%s]" % output

    def __iter__(self):
        for resource in self.resources:
            yield(resource)

    def pre_action(self, action, **kwargs):
        """
        Call the pre_action of each resource driver in the resource set.
        """
        tags = kwargs.get("tags", set())
        xtags = kwargs.get("xtags", set())
        xtypes = kwargs.get("xtypes")
        types = kwargs.get("types")
        resources = self.action_resources(action, tags, xtags, xtypes, types)
        if len(resources) == 0:
            return []
        types_done = []
        types_aborted = []
        for resource in resources:
            if resource.type in types_done:
                continue
            types_done.append(resource.type)
            if not hasattr(resource, "pre_action"):
                continue
            if self.svc.options.dry_run:
                self.log.info("pre_action %s" % resource.type)
                continue
            try:
                resource.pre_action(action)
            except ex.excAbortAction:
                types_aborted.append(resource.type)
        return types_aborted

    def post_action(self, action, **kwargs):
        """
        Call the post_action of each resource driver in the resource set.
        """
        tags = kwargs.get("tags", set())
        xtags = kwargs.get("xtags", set())
        xtypes = kwargs.get("xtypes")
        types = kwargs.get("types")
        resources = self.action_resources(action, tags, xtags, xtypes, types)
        if len(resources) == 0:
            return []
        types_done = []
        types_aborted = []
        for resource in resources:
            if resource.type in types_done:
                continue
            types_done.append(resource.type)
            if not hasattr(resource, "post_action"):
                continue
            if self.svc.options.dry_run:
                self.log.info("post_action %s" % resource.type)
                continue
            try:
                resource.post_action(action)
            except ex.excAbortAction:
                types_aborted.append(resource.type)
        return types_aborted

    def purge_status_last(self):
        """
        Purge the on-disk status cache of each resource of the resourceset.
        """
        for resource in self.resources:
            resource.purge_status_last()

    def is_disabled(self):
        if self.svc.disabled:
            return self.svc.disabled
        return self.disabled

    def status(self, **kwargs):
        """
        Return the aggregate status a ResourceSet.
        """
        agg_status = rcStatus.Status()
        for resource in self.resources:
            if resource.is_disabled():
                continue
            if not self.svc.encap and resource.encap:
                # don't evaluate encap service resources
                continue

            try:
                status = resource.status(**kwargs)
            except:
                import traceback
                exc = sys.exc_info()
                print(exc[0], exc[1], traceback.print_tb(exc[2]))
                status = rcStatus.NA

            agg_status += status
        return agg_status.status

    @staticmethod
    def tag_match(rtags, keeptags):
        """
        A helper method to determine if resource has a tag in the specified
        list of tags.
        """
        if len(keeptags) == 0:
            return True
        for tag in rtags:
            if tag in keeptags:
                return True
        return False

    def has_resource_with_types(self, types, strict=False):
        """
        Return True if the resourceset has at least one resource of the
        specified type.
        """
        for resource in self.resources:
            if resource.type in types:
                return True
            if not strict and "." in resource.type and \
               resource.type.split(".")[0] in types:
                return True
        return False

    def has_encap_resources(self):
        """
        Return True if the resourceset has at least one encap resource
        """
        resources = [True for res in self.resources if res.encap]
        if len(resources) == 0:
            return False
        return True

    def sort_resources(self, resources, action):
        """
        Return resources after a resourceset-specific sort.
        To be implemented by child classes if desired.
        """
        if action in ["start", "startstandby", "provision"] or \
           self.rid.startswith("sync"):
            resources.sort()
        else:
            resources.sort(reverse=True)
        return resources

    def action_resources(self, action, tags, xtags, xtypes, types):
        """
        Return resources to execute the action on.
        """
        if len(xtags) > 0:
            resources = []
            for res in self.resources:
                if not self.tag_match(res.tags, xtags):
                    resources.append(res)
                    continue
                if self.svc.command_is_scoped() and \
                   res.rid in self.svc.action_rid_before_depends and \
                   len(self.svc.action_rid_dependencies(action, res.rid)) == 0:
                    resources.append(res)
                    continue
        else:
            resources = self.resources

        resources = [res for res in resources if self.tag_match(res.tags, tags)]
        self.log.debug("resources after tags[%s] filter: %s",
                       ','.join(tags),
                       ','.join([res.rid for res in resources]))

        disabled_rids = [res.rid for res in resources if res.is_disabled() and not res.skip]
        if len(disabled_rids) > 0:
            self.log.info("%s disabled", ",".join(disabled_rids))

        resources = [res for res in resources if not res.is_disabled()]
        self.log.debug("resources after 'disable' filter: %s",
                       ','.join([res.rid for res in resources]))

        resources = [res for res in resources if \
                     types is None or \
                     res.type in types or \
                     res.type.split(".")[0] in types]
        resources = [res for res in resources if \
                     xtypes is None or \
                     (res.type not in xtypes and \
                      res.type.split(".")[0] not in xtypes)]

        if action == "startstandby":
            # filter out resource not in standby mode
            resources = [res for res in resources if res.standby]

        resources = self.sort_resources(resources, action)

        if action == "provision":
            # make sure scsi reservation are taken after their co-resource is
            # provisioned, so the disk list to reserv will not be empty.
            resources = [res for res in resources if res.type != "disk.scsireserv"] + \
                        [res for res in resources if res.type == "disk.scsireserv"]

        return resources

    def action(self, action, **kwargs):
        """
        Call the action method for each resource of the ResourceSet.
        Handle parallel or serialized execution plans.
        """
        tags = kwargs.get("tags", set())
        xtags = kwargs.get("xtags", set())
        xtypes = kwargs.get("xtypes")
        types = kwargs.get("types")

        if self.parallel:
            # verify we can actually do parallel processing, fallback to serialized
            try:
                from multiprocessing import Process
                if rcEnv.sysname == "Windows":
                    from multiprocessing import set_executable
                    set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
            except:
                self.parallel = False

        resources = self.action_resources(action, tags, xtags, xtypes, types)
        barrier = None

        if not self.svc.options.dry_run and \
           self.parallel and len(resources) > 1 and \
           action not in ["presync", "postsync"]:
            procs = {}
            for resource in resources:
                if not resource.can_rollback and action == "rollback":
                    continue
                if resource.skip or resource.is_disabled():
                    continue
                proc = Process(target=self.action_job, args=(resource, action,))
                proc.start()
                resource.log.info("action %s started in child process %d"%(action, proc.pid))
                procs[resource.rid] = proc
                if self.svc.options.upto and resource.rid == self.svc.options.upto:
                    barrier = "reached 'up to %s' barrier" % resource.rid
                    break
                if self.svc.options.downto and resource.rid == self.svc.options.downto:
                    barrier = "reached 'down to %s' barrier" % resource.rid
                    break
            for proc in procs.values():
                proc.join()
            err = []
            for resource in resources:
                if resource.rid not in procs:
                    continue
                proc = procs[resource.rid]
                if proc.exitcode == 1 and not resource.optional:
                    err.append(resource.rid)
                elif proc.exitcode == 2:
                    # can_rollback resource property is lost with the thread
                    # the action_job tells us what to do with it through its exitcode
                    resource.can_rollback = True
            if len(err) > 0:
                raise ex.excError("%s non-optional resources jobs returned "
                                  "with error" % ",".join(err))
        else:
            if self.svc.options.dry_run and \
               self.parallel and len(resources) > 1 and \
               action not in ["presync", "postsync"]:
                self.log.info("entering parallel subset")
            for resource in resources:
                try:
                    resource.action(action)
                except ex.excAbortAction as exc:
                    msg = str(exc)
                    if msg != "":
                        resource.log.warning(msg)
                    resource.log.warning("abort action on resource set")
                    break
                except ex.excContinueAction as exc:
                    msg = str(exc)
                    if msg != "":
                        resource.log.info(msg)
                    resource.log.info("continue action on resource set")
                except ex.excError as exc:
                    msg = str(exc)
                    if msg != "":
                        resource.log.error(msg)
                        # prevent re-logging
                        exc.value = ""
                    raise exc
                if self.svc.options.upto and resource.rid == self.svc.options.upto:
                    barrier = "reached 'up to %s' barrier" % resource.rid
                    break
                if self.svc.options.downto and resource.rid == self.svc.options.downto:
                    barrier = "reached 'down to %s' barrier" % resource.rid
                    break

        if barrier:
            raise ex.excEndAction(barrier)

    def action_job(self, resource, action):
        """
        The worker job used for parallel execution of a resource action in
        a resource set.
        """
        try:
            getattr(resource, 'action')(action)
        except Exception as exc:
            self.log.error(str(exc))
            sys.exit(1)
        if resource.can_rollback:
            sys.exit(2)
        sys.exit(0)

    def all_skip(self, action):
        """
        Return False if any resource will not skip the action.
        """
        for resource in self.resources:
            if not resource.skip_resource_action(action):
                return False
        return True

    @lazy
    def log(self):
        extra = {
            "path": self.svc.path,
            "node": rcEnv.nodename,
            "sid": rcEnv.session_uuid,
            "cron": self.svc.options.cron,
            "subset": self.rid,
        }
        return logging.LoggerAdapter(self.logger, extra)

    @lazy
    def logger(self):
        """
        Lazy init for the resource logger.
        """
        l = [
            rcEnv.nodename,
            self.svc.name,
            self.rid
        ]
        if self.svc.namespace:
            l.insert(1, self.svc.namespace.lower())
        name = ".".join(l)
        return logging.getLogger(name)

if __name__ == "__main__":
    pass

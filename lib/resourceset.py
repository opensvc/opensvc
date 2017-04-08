"""
Defines the resource set class, which is a collection of resources.
"""
from __future__ import print_function

import os
import sys
import logging

import rcExceptions as ex
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
                 type=None,
                 resources=None,
                 parallel=False,
                 optional=False,
                 disabled=False,
                 tags=None):
        self.parallel = parallel
        self.svc = None
        self.type = type
        self.optional = optional
        self.disabled = disabled
        self.tags = tags
        self.resources = []
        if resources is not None:
            for resource in resources:
                self += resource

    def __lt__(self, other):
        return self.type < other.type

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
        output = "resSet %s [" % str(self.type)
        for resource in self.resources:
            output += " (%s)" % (resource.__str__())
        return "%s]" % output

    def __iter__(self):
        for resource in self.resources:
            yield(resource)

    def pre_action(self, action):
        """
        Call the pre_action of each resource driver in the resource set.
        """
        if len(self.resources) == 0:
            return
        types_done = []
        for resource in self.resources:
            if resource.type in types_done:
                continue
            types_done.append(resource.type)
            if not hasattr(resource, "pre_action"):
                continue
            resource.pre_action(action)

    def post_action(self, action):
        """
        Call the post_action of each resource driver in the resource set.
        """
        if len(self.resources) == 0:
            return
        types_done = []
        for resource in self.resources:
            if resource.type in types_done:
                continue
            types_done.append(resource.type)
            if not hasattr(resource, "post_action"):
                continue
            resource.post_action(action)

    def purge_status_last(self):
        """
        Purge the on-disk status cache of each resource of the resourceset.
        """
        for resource in self.resources:
            resource.purge_status_last()

    def status(self, **kwargs):
        """
        Return the aggregate status a ResourceSet.
        """
        agg_status = rcStatus.Status()
        for resource in self.resources:
            if resource.is_disabled():
                continue
            if not resource.svc.encap and 'encap' in resource.tags:
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
        resources = [res for res in self.resources if \
                     self.tag_match(res.tags, set(['encap']))]
        if len(resources) == 0:
            return False
        return True

    def sort_resources(self, resources, action):
        """
        Return resources after a resourceset-specific sort.
        To be implemented by child classes if desired.
        """
        if action in ["fs", "start", "startstandby", "provision"] or self.type.startswith("sync"):
            resources.sort()
        else:
            resources.sort(reverse=True)
        return resources

    def action_resources(self, action, tags, xtags):
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
                       str(tags), ','.join([res.rid for res in resources]))

        resources = [res for res in resources if not res.disabled]
        self.log.debug("resources after 'disable' filter: %s",
                       ','.join([res.rid for res in resources]))

        if action == "startstandby":
            # filter out resource not in standby mode
            resources = [res for res in resources if rcEnv.nodename in res.always_on]

        resources = self.sort_resources(resources, action)
        return resources

    def action(self, action, **kwargs):
        """
        Call the action method for each resource of the ResourceSet.
        Handle parallel or serialized execution plans.
        """
        tags = kwargs.get("tags", set())
        xtags = kwargs.get("xtags", set())

        if self.parallel:
            # verify we can actually do parallel processing, fallback to serialized
            try:
                from multiprocessing import Process
                if rcEnv.sysname == "Windows":
                    from multiprocessing import set_executable
                    set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
            except:
                self.parallel = False

        resources = self.action_resources(action, tags, xtags)

        if not self.svc.options.dry_run and \
           self.parallel and len(resources) > 1 and \
           action not in ["presync", "postsync"]:
            procs = {}
            for resource in resources:
                if not resource.can_rollback and action == "rollback":
                    continue
                if resource.skip or resource.disabled:
                    continue
                proc = Process(target=self.action_job, args=(resource, action,))
                proc.start()
                resource.log.info("action %s started in child process %d"%(action, proc.pid))
                procs[resource.rid] = proc
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
                    if msg is not "":
                        resource.log.warning(msg)
                    resource.log.warning("abort action on resource set")
                    break
                except ex.excContinueAction as exc:
                    msg = str(exc)
                    if msg is not "":
                        resource.log.info(msg)
                    resource.log.info("continue action on resource set")
                except ex.excError as exc:
                    resource.log.error(str(exc))
                    raise

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
        """
        Lazy init for the resource logger.
        """
        return logging.getLogger(self.type)

if __name__ == "__main__":
    pass

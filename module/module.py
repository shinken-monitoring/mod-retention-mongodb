#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.

"""
This is a scheduler module to save host/service retention data into a mongodb database
"""

import time
import cPickle
import datetime

import base64
from multiprocessing import Process, cpu_count

try:
    from pymongo import MongoClient,version
except ImportError:
    MongoClient = None

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['scheduler'],
    'type': 'mongodb_retention',
    'external': False,
    }

def get_instance(plugin):
    """
    Called by the plugin manager to get a broker
    """
    logger.debug("Get a Mongodb retention scheduler module for plugin %s" % plugin.get_name())
    if not MongoClient:
        raise Exception('Could not use the pymongo module. Please verify your pymongo install.')
    uri = plugin.uri
    database = plugin.database
    replica_set = getattr(plugin, 'replica_set', '')
    instance = Mongodb_retention_scheduler(plugin, uri, database, replica_set)
    return instance


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class Mongodb_retention_scheduler(BaseModule):
    def __init__(self, modconf, uri, database, replica_set):
        BaseModule.__init__(self, modconf)
        self.uri = uri
        self.database = database
        self.replica_set = replica_set
        self.max_workers = 4
        # Older versions don't handle replicasets and don't have the fsync option
        if version < 2:
            logger.error('[MongodbRetention] Your pymongo lib is too old. '
                         'Please install at least a 2.x+ version.')
            return None


    def init(self):
        """
        Called by Scheduler to say 'let's prepare yourself guy'
        """
        logger.debug("Initialization of the mongodb module")

        if self.replica_set:
            self.con = MongoClient(self.uri, replicaSet=self.replica_set, fsync=False)
        else:
            self.con = MongoClient(self.uri, fsync=False)

        #self.con = Connection(self.uri)
        # Open a gridfs connection
        self.db = getattr(self.con, self.database)
        self.hosts_fs = getattr(self.db, 'retention_hosts_raw') #GridFS(self.db, collection='retention_hosts')
        self.services_fs = getattr(self.db, 'retention_services_raw')


    def job(self, all_data, wid, offset):
        t0 = time.time()
        # Reinit the mongodb connection if need
        self.init()
        all_objs = {'hosts':{}, 'services':{}}
        date = datetime.datetime.utcnow()

        hosts = all_data['hosts']
        services = all_data['services']
        
        # Prepare the encoding for all managed hosts
        i = -1
        for h_name in hosts:
            # Only manage the worker id element of the offset (number of workers)
            # elements
            i += 1
            if (i % offset) != wid:
                continue
            h = hosts[h_name]
            key = "HOST-%s" % h_name
            val = cPickle.dumps(h, protocol=cPickle.HIGHEST_PROTOCOL)
            val2 = base64.b64encode(val)            
            # We save it in the Gridfs for hosts
            all_objs['hosts'][key] = {'_id':key, 'value':val2, 'date':date}

        i = -1
        for (h_name, s_desc) in services:
            i += 1
            # Only manage the worker id element of the offset (number of workers)
            # elements
            if (i % offset) != wid:
                continue
            s = services[(h_name, s_desc)]
            key = "SERVICE-%s,%s" % (h_name, s_desc)
            # space are not allowed in a key.. so change it by SPACE token
            key = key.replace(' ', 'SPACE')
            val = cPickle.dumps(s, protocol=cPickle.HIGHEST_PROTOCOL)
            val2 = base64.b64encode(val)
            all_objs['services'][key] = {'_id':key, 'value':val2, 'date':date}
        
        if len(all_objs['hosts']) != 0:
            t2 = time.time()
            self.hosts_fs.remove({ '_id': { '$in': all_objs['hosts'].keys()}}, w=0, j=False, fsync=False)
            
            # Do bulk insert of 100 elements (~100K insert)
            lsts = list(chunks(all_objs['hosts'].values(), 100))
            for lst in lsts:
                fd = self.hosts_fs.insert(lst, w=0, j=False, fsync=False)

        if len(all_objs['services']) != 0:
            t2 = time.time()
            self.services_fs.remove({ '_id': { '$in': all_objs['services'].keys()}}, w=0, j=False, fsync=False)
            # Do bulk insert of 100 elements (~100K insert)
            lsts = list(chunks(all_objs['services'].values(), 100))
            for lst in lsts:
                fd = self.services_fs.insert(lst, w=0, j=False, fsync=False)        

        # Return and so quit this sub-process
        return


    def hook_save_retention(self, daemon):
        """
        main function that is called in the retention creation pass
        """

        try:
            self.max_workers = cpu_count()
        except NotImplementedError:
            pass
        
        t0 = time.time()
        logger.debug("[MongodbRetention] asking me to update the retention objects")

        all_data = daemon.get_retention_data()

        t3 = time.time()
        processes = []
        for i in xrange(self.max_workers):
            proc = Process(target=self.job, args=(all_data, i, self.max_workers))
            proc.start()
            processes.append(proc)

        # Allow 30s to join the sub-processes, should be enough
        for proc in processes:
            proc.join(30)

        logger.info("Retention information updated in Mongodb (%.2fs)" % (time.time() - t0))

    
    # Should return if it succeed in the retention load or not
    def hook_load_retention(self, daemon):

        # Now the new redis way :)
        logger.debug("MongodbRetention] asking me to load the retention objects")

        # We got list of loaded data from retention uri
        ret_hosts = {}
        ret_services = {}

        found_hosts = {}
        found_services = {}

        for h in self.hosts_fs.find():
            val = h.get('value', None)
            if val is not None:
                found_hosts[h.get('_id')] = val

        for s in self.services_fs.find():
            val = s.get('value', None)
            if val is not None:
                found_services[s.get('_id')] = val

        for h in daemon.hosts:
            key = "HOST-%s" % h.host_name
            if key in found_hosts:
                val = found_hosts[key]
                val = base64.b64decode(val)
                val = cPickle.loads(val)
                ret_hosts[h.host_name] = val

        for s in daemon.services:
            key = "SERVICE-%s,%s" % (s.host.host_name, s.service_description)
            # space are not allowed in memcache key.. so change it by SPACE token
            key = key.replace(' ', 'SPACE')
            if key in found_services:
                val = found_services[key]
                val = base64.b64decode(val)
                val = cPickle.loads(val)
                ret_services[(s.host.host_name, s.service_description)] = val
        
        all_data = {'hosts': ret_hosts, 'services': ret_services}

        # Ok, now comme load them scheduler :)
        daemon.restore_retention_data(all_data)

        logger.info("[MongodbRetention] Retention objects loaded successfully.")

        return True

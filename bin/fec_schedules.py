'''
FEC Modular Input Script

Copyright (C) 2015 Splunk, Inc.
dgreenwood@splunk.com
All Rights Reserved

'''
import os

import sys,logging
from time import sleep
import xml.dom.minidom, xml.sax.saxutils
import urllib2
import json

#set up logging
import time

logging.root
logging.root.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s %(message)s')
handler = logging.StreamHandler(stream=sys.stderr)
handler.setFormatter(formatter)
logging.root.addHandler(handler)


SCHEME = """<scheme>
    <title>FEC Schedules</title>
    <description>Input to collect data from the FEC API /schedules endpoints.</description>
    <use_external_validation>true</use_external_validation>
    <streaming_mode>xml</streaming_mode>
    <use_single_instance>false</use_single_instance>

    <endpoint>
        <args>    
            <arg name="name">
                <title>FEC Schedules Input Name</title>
                <description>FEC Schedules Input Name</description>
            </arg>
                   
            <arg name="fec_key">
                <title>FEC API Key</title>
                <description>Get this from api.open.fec.gov</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>true</required_on_create>
            </arg>

            <arg name="get_schedules_schedule_a">
                <title>Collect Schedule A data</title>
                <description>Index data Schedule A data? : true | false</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>

            <arg name="get_schedules_schedule_b">
                <title>Collect Schedule B data</title>
                <description>Index data Schedule B data? : true | false</description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>

            <arg name="polling_interval">
                <title>Polling interval</title>
                <description>Interval time in hours to poll the endpoint/description>
                <required_on_edit>false</required_on_edit>
                <required_on_create>false</required_on_create>
            </arg>
            
        </args>
    </endpoint>
</scheme>
"""

class Fec(object):

    URL_BASE = "https://api.open.fec.gov/v1/schedules/"
    ENDPOINT_A = "schedule_a"
    ENDPOINT_B = "schedule_b"
    DEFAULT_PARAMS = "?sort_hide_null=false&per_page=100"

    def __init__(self, key, poll_endpoint_a_str, poll_endpoint_b_str, polling_interval, checkpoint_dir):
        self.api_key = key
        self.poll_endpoint_a = True if poll_endpoint_a_str == u'1' else False
        self.poll_endpoint_b = True if poll_endpoint_b_str == u'1' else False
        if not polling_interval or polling_interval<0:
            #every 24 hours by default
            polling_interval = 24
        else:
            self.polling_interval = polling_interval #polling interval provided in hours

        self.polling_interval *= 60 * 60
        self.checkpoint_dir = checkpoint_dir


    def _build_enpoint_a_url(self):
        return Fec.URL_BASE+Fec.ENDPOINT_A+Fec.DEFAULT_PARAMS+"&api_key={}".format(self.api_key)

    def _build_enpoint_b_url(self):
        return Fec.URL_BASE+Fec.ENDPOINT_B+Fec.DEFAULT_PARAMS+"&api_key={}".format(self.api_key)

    def _get_json_response(self, url, last_index=None):
        """
        Method to load json response from API and return the response converted to python dictionary.
        Takes care of exception handling and pauses querying the endpoint when limit has been reached.

        :param url: URL to endpoint with default parameters (without last_index param)
        :param last_index: param specifying which page to load next
        :return: json dictionary with endpoint response
        """

        if last_index:
            url += '&last_index={}'.format(str(last_index))

        logging.info("processing url: %s", url)
        #fetch
        req = urllib2.Request(url)
        opener = urllib2.build_opener()

        try:
            resp = opener.open(req)
        except (urllib2.HTTPError, urllib2.URLError) as e:
            logging.exception("Error in fetching the url: %s" % url)
            #retry

            return False
        else:
            # 200
            queries_left = resp.info().getheader('X-RateLimit-Remaining')

            if not queries_left is None: #header exists
                queries_left = int(queries_left)

            if queries_left == 1:
                #wait for an hour
                logging.info("Request limit achieved,")
                sleep(61*60)

            return json.loads(resp.read())

    def _parse_response(self, response_json, pk_field):

        last_index = None # it's a "page" checkpoint, to know what page to fetch in order to process new records

        try:
            results = response_json['results']
            last_indexes = response_json['pagination']['last_indexes']
            if last_indexes:
                last_index = last_indexes['last_index']
                #api doesn't accept last index value provided by itself
                #e.g. last index from api: {u'last_index': 138873463.0}
                #we need to truncate .0
                last_index = int(last_index)
            else:
                #last page
                logging.debug("Last indexes not found - it's last page")

            for result in results:
                #last_index is a primary key of records
                last_index = result[pk_field]
                #print results to Splunk
                print_xml_single_instance_mode(str(result))


        except KeyError:
            raise Exception('Invalid response')

        return last_index #returns last parsed record id

    def _fetch_urls(self, url, pk_field, endpoint, index_to_begin_with=None):

        continue_processing = True
        last_index = index_to_begin_with

        retry_counter = -1
        while continue_processing:
            response_json = None
            while not response_json:
                retry_counter += 1
                response_json = self._get_json_response(url, last_index=last_index)
                if retry_counter > 3:
                    raise Exception("Unable to process url: %s, exiting..." % url)

            retry_counter = -1 # reset retry counter

            #new_last_index is an id of last result on last page fetched or None if response empty
            new_last_index = self._parse_response(response_json, pk_field = pk_field)

            if not new_last_index:
                #such response has been returned:
                #{"results":[],"pagination":{"per_page":100,"pages":457609,"count":45760809,"last_indexes":null},"api_version":"1.0"}
                continue_processing = False
                if last_index:
                    save_checkpoint(self.checkpoint_dir, endpoint, last_index)
            elif new_last_index:
                #response contained results and last_indexes information
                last_index = new_last_index
                #continue processing

    def run(self):
        try:
            while True:
                if self.poll_endpoint_a:
                    self.last_index_endpoint_a = load_checkpoint(self.checkpoint_dir, endpoint=Fec.ENDPOINT_A)
                    url = self._build_enpoint_a_url()
                    self._fetch_urls(url, pk_field = 'sched_a_sk', endpoint=Fec.ENDPOINT_A, index_to_begin_with=self.last_index_endpoint_a)
                if self.poll_endpoint_b:
                    self.last_index_endpoint_b = load_checkpoint(self.checkpoint_dir, endpoint=Fec.ENDPOINT_B)
                    url = self._build_enpoint_b_url()
                    self._fetch_urls(url, pk_field = 'sched_b_sk', endpoint=Fec.ENDPOINT_B, index_to_begin_with=self.last_index_endpoint_b)
                logging.info("sleeping for: %s", str(self.polling_interval))
                time.sleep(int(self.polling_interval))
                logging.info("running after sleep interval")
        except RuntimeError, e:
            logging.exception("Unexpected error happened")
            sys.exit(2)


def _get_checkpoint_file(checkpoint_dir, endpoint):
    return os.path.join(checkpoint_dir, endpoint)

# simply creates a checkpoint file indicating that the last_index was checkpointed
def save_checkpoint(checkpoint_dir, endpoint, last_index):
    chk_file = _get_checkpoint_file(checkpoint_dir, endpoint)
    logging.info("Saving checkpoint file=%s", chk_file)
    f = open(chk_file, "w")
    f.write(str(last_index))
    f.close()

def load_checkpoint(checkpoint_dir, endpoint):

    chk_file = _get_checkpoint_file(checkpoint_dir, endpoint)
    # try to open this file
    try:
        f = open(chk_file, "r")
        last_index = f.read()
        logging.info("Checkpoint for endpoint: %s is %s", endpoint, last_index)
    except:
        # assume that this means the checkpoint is not there
        logging.info("Checkpoint for endpoint: %s doesn't exist", endpoint)
        return None
    return last_index

def do_validate():
    config = get_validation_config() 
    #TODO
    #if error , print_validation_error & sys.exit(2) 
    
def do_run():
    config = get_input_config()

    fec = Fec(key=config["fec_key"],
              poll_endpoint_a_str=config["get_schedules_schedule_a"],
              poll_endpoint_b_str=config["get_schedules_schedule_b"],
              polling_interval=int(config["polling_interval"]),
              checkpoint_dir = config["checkpoint_dir"])

    logging.info("Run fetching urls")
    fec.run()


# prints validation error data to be consumed by Splunk
def print_validation_error(s):
    print "<error><message>%s</message></error>" % xml.sax.saxutils.escape(s)
    
# prints XML stream
def print_xml_single_instance_mode(s):
    print "<stream><event><data>%s</data></event></stream>" % xml.sax.saxutils.escape(s)
    
# prints XML stream
def print_xml_multi_instance_mode(s,stanza):
    print "<stream><event stanza=""%s""><data>%s</data></event></stream>" % stanza,xml.sax.saxutils.escape(s)
    
# prints simple stream
def print_simple(s):
    print "%s\n" % s
    
def usage():
    print "usage: %s [--scheme|--validate-arguments]"
    logging.error("Incorrect Program Usage")
    sys.exit(2)

def do_scheme():
    print SCHEME

#read XML configuration passed from splunkd, need to refactor to support single instance mode
def get_input_config():
    #
    # config = {"fec_key":"jDfgacAyopgr2Ig6dKkU250JH0bR5DfuUqSiCzY4",
    #           u'get_schedules_schedule_a': u'1',
    #             u'get_schedules_schedule_b': u'0',
    #             "polling_interval": 15,
    #             "checkpoint_dir": '/home/mstrecker/'}
    #
    # return config

    config = {}

    try:
        # read everything from stdin
        config_str = sys.stdin.read()

        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement
        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            logging.debug("XML: found configuration")
            stanza = conf_node.getElementsByTagName("stanza")[0]
            if stanza:
                stanza_name = stanza.getAttribute("name")
                if stanza_name:
                    logging.debug("XML: found stanza " + stanza_name)
                    config["name"] = stanza_name

                    params = stanza.getElementsByTagName("param")
                    for param in params:
                        param_name = param.getAttribute("name")
                        logging.debug("XML: found param '%s'" % param_name)
                        if param_name and param.firstChild and \
                           param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                            data = param.firstChild.data
                            config[param_name] = data
                            logging.debug("XML: '%s' -> '%s'" % (param_name, data))

        checkpnt_node = root.getElementsByTagName("checkpoint_dir")[0]
        if checkpnt_node and checkpnt_node.firstChild and \
           checkpnt_node.firstChild.nodeType == checkpnt_node.firstChild.TEXT_NODE:
            config["checkpoint_dir"] = checkpnt_node.firstChild.data

        if not config:
            raise Exception, "Invalid configuration received from Splunk."

        
    except Exception, e:
        raise Exception, "Error getting Splunk configuration via STDIN: %s" % str(e)

    logging.info("CONFIG:")
    logging.info(str(config))

    return config

#read XML configuration passed from splunkd, need to refactor to support single instance mode
def get_validation_config():
    val_data = {}

    # read everything from stdin
    val_str = sys.stdin.read()

    # parse the validation XML
    doc = xml.dom.minidom.parseString(val_str)
    root = doc.documentElement

    logging.debug("XML: found items")
    item_node = root.getElementsByTagName("item")[0]
    if item_node:
        logging.debug("XML: found item")

        name = item_node.getAttribute("name")
        val_data["stanza"] = name

        params_node = item_node.getElementsByTagName("param")
        for param in params_node:
            name = param.getAttribute("name")
            logging.debug("Found param %s" % name)
            if name and param.firstChild and \
               param.firstChild.nodeType == param.firstChild.TEXT_NODE:
                val_data[name] = param.firstChild.data

    return val_data

if __name__ == '__main__':
     
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":           
            do_scheme()
        elif sys.argv[1] == "--validate-arguments":
            do_validate()
        else:
            usage()
    else:
        do_run()
        
    sys.exit(0)

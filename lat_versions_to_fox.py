#!/usr/bin/env python

"""lat_versions_to_fox.py: Fetches versions of archived objects from LAT (CMDI) stack postgres db and adds them to FOXML. Assumes fresh FOXML files resulting from lat2fox, with no versioned objects present. Run before import into Fedora."""

__author__ = "Paul Trilsbeek"
__license__ = "GPL3"
__version__ = "0.1"

import logging
import subprocess
import os
import re
from lxml import etree as ET
import psycopg2
import psycopg2.extras

# connect to corpusstructure db, use readonly DB user that requires no password
DB_HOST = 'localhost'
DB_NAME = 'corpusstructure'
DB_USER = 'imdiArchive'

# options below are used to fetch md5 directly from versity filesystem, in case it is present, or compute otherwise
STORAGE_SERVER = 'storagehost'
STORAGE_USER = 'corpman'
MD5_COMMAND = '/opt/vsm/sbin/slssum'
MD5_FALLBACK = 'md5sum'

FOX_DIR = '/app/flat/import/fox/'
TARGET_FOX_DIR = '/app/flat/import/fox_with_versions/'
VERSIONS_ROOT_DIR = '/lat/corpora/version-archive/'
VERSIONS_BASE_URL = 'https://latserver.org/version-archive/'

# create LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
CH = logging.StreamHandler()
CH.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
CH.setFormatter(FORMATTER)
LOGGER.addHandler(CH)

# function to return empty string if value is None
def xstr(s):
    if s is None:
        return ''
    return str(s)

CONN_STRING = "host=" + DB_HOST + " dbname=" + DB_NAME + " user=" + DB_USER
CONN = psycopg2.connect(CONN_STRING)
CUR = CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)
# get number of rows in versions table
CUR.execute("SELECT count(*) FROM versions;")
VERSIONS_TABLE_SIZE_RESULT = CUR.fetchone()
# get version sequence from db
VERSIONS_TABLE_SIZE = (VERSIONS_TABLE_SIZE_RESULT[0])
ROW = 0
ERROR_STATUS = ""
while ROW < VERSIONS_TABLE_SIZE:
    CUR.execute("SELECT * FROM versions OFFSET %s LIMIT 1;" % ROW) # walk through versions table row by row
    for record in CUR:
        nodeid = record[0]
        #nodeid = "MPI" + str(nodenumber) + "#" # cs table contains just node numbers, version table contains nodeids with MPI prefix and hash suffix
        nodenumber = nodeid[3:-1]
        version_sequence = []
        version_sequence.append(nodeid)
        #print("nodeid: %s" % (nodeid))
        while nodeid is not None:
            CUR.execute("SELECT olderversion FROM versions where nodeid = '%s';" % nodeid) # check if there is an older version in the db for the given nodeid
            older_result = CUR.fetchone()
            if older_result is not None:
                older = older_result[0]
                #print("older version: %s" % (older))
                if older is not None:
                    version_sequence.insert(0, older) # populate list in chronological order
                nodeid = older
            else:
                nodeid = None
        versions_string = ','.join(map(str, version_sequence))
        LOGGER.info("version sequence: %s", versions_string)
        if len(version_sequence) > 1: # only intersted in version sequences with older versions, i.e. more than one value
            versions_string = ','.join(map(str, version_sequence))
            #LOGGER.info("version sequence: %s", versions_string)
            nodeid = version_sequence[-1] # last nodeid in list for current version
            nodenumber = nodeid[3:-1] # strip the prefix and suffix again to query archiveobjects table
            CUR.execute("SELECT pid FROM archiveobjects where nodeid ='%s';" % nodenumber) # get handel pid of current node, needed to derive FOXML filename
            current_pid_result = CUR.fetchone()
            current_pid = current_pid_result[0]
            if current_pid:
                # transform pid into FOXML filename
                foxml_filename = current_pid.replace("hdl:", "")
                foxml_filename = foxml_filename.replace("/", "_")
                foxml_filename = foxml_filename.replace("-", "_")
                foxml_filename = foxml_filename.replace("@format=imdi", "_CMD")
                foxml_filename = "lat_" + foxml_filename + ".xml"
                # find foxml file in fox directory
                fox_path = ""
                for root, dirs, files in os.walk(FOX_DIR):
                    for file in files:
                        if file == foxml_filename:
                            fox_path = os.path.join(root, file)
                if not fox_path:
                    LOGGER.error("FOXML file not found: %s", foxml_filename) # mismatch between CS DB and FOXML on file system
                else:
                    fox_tree = ET.parse(fox_path) # parse the foxml file using elementTree
                    fox_root = fox_tree.getroot()
                    ERROR_STATUS = ""
                    fedora_version_number = 0
                    # current version needs to get highest ID number
                    highest_version_number = len(version_sequence) - 1
                    if current_pid[-11:] == "format=imdi": # versioned metadata
                        fid = "CMD." + str(highest_version_number)
                        cmd_datastream_current_version = fox_root.find(".//{info:fedora/fedora-system:def/foxml#}datastreamVersion[@ID='CMD.0']")
                        cmd_datastream_current_version.set('ID', fid)
                    else: # versioned OBJ
                        fid = "OBJ." + str(highest_version_number)
                        obj_datastream_current_version = fox_root.find(".//{info:fedora/fedora-system:def/foxml#}datastreamVersion[@ID='OBJ.0']")
                        obj_datastream_current_version.set('ID', fid)
                    for version in version_sequence[:-1]: # skip last one since thats the current version already present in the FOXML
                        versionnumber = version[3:-1] # strip the prefix and suffix again to query archiveobjects table
                        # fetch version info from archiveobject and corpusnodes tables. Datastream version needs ID (incl. Fedora version number), LABEL (filename), CREATED (date created, ISO-8601),
                        # MIMETYPE, "contentlocation" subelement with file URL, optionally "contentdigest" subelement with MD5 checksum
                        CUR.execute("SELECT * FROM archiveobjects where nodeid ='%s';" % versionnumber)
                        rec = CUR.fetchone()
                        filetime = xstr(rec['filetime'])
                        filetime = filetime.replace(" ", "T") + ".000Z" # needs complete ISO-8601 timestamp. Todo: check timezone, maybe need to convert to UTC
                        url = rec['url'] # we'll be using URL to derive localpath and label. Localpath doesn't seem to be always present, title and name often seem to contain values from before versioning.
                        if not url:
                            LOGGER.error("Corpusstructure DB returns no URL for node: %s", version)
                        else:
                            regex = '(?<=' + VERSIONS_BASE_URL + ').*'
                            localpath_regex_match = re.search(regex, url)
                            localpath = VERSIONS_ROOT_DIR + localpath_regex_match.group(0)
                            localpath = localpath.replace("&outFormat=imdi", "")
                            localpath_with_prefix = "file:" + localpath
                            version_file_missing = False
                            imdi = False
                            if not os.path.isfile(localpath): # check whether file is actually there, report if not. Old metadata versions might only exist as IMDI, even though CS DB says CMDI.
                                imdi_path = localpath.replace(".cmdi", ".imdi")
                                if not os.path.isfile(imdi_path):
                                    LOGGER.error("file does not exist on file system: %s", localpath)
                                    version_file_missing = True
                                else:
                                    localpath = imdi_path
                                    imdi = True
                                    LOGGER.info("metadata file is in IMDI format, not CMDI: %s", localpath)
                            label = os.path.basename(localpath)
                            if not label[:1] == 'v': # filenames of versioned files should have a prefix starting with the letter v. Report if this is not the case.
                                LOGGER.warning("version filename does not start with a v: %s", localpath)
                            checksum = xstr(rec['checksum']) # might be emtpy, in case of OBJ we should then compute it later
                            CUR.execute("SELECT * FROM corpusnodes where nodeid ='%s';" % versionnumber)
                            rec = CUR.fetchone()
                            if rec:
                                mimetype = xstr(rec['format'])
                                if current_pid[-11:] == "format=imdi": # versioned metadata file, needs to be treated differently as inline XML datastream.
                                    md_datastream = fox_root.find(".//{info:fedora/fedora-system:def/foxml#}datastream[@ID='CMD']") # find the CMD datastream in the FOXML
                                    fid = "CMD." + str(fedora_version_number)
                                    if imdi:
                                        label = "IMDI Record for this object"
                                    else:
                                        label = "CMD Record for this object"
                                        mimetype = "application/x-cmdi+xml" # mimetype for CMDI in hybrid LAT stack DB is actually application/x-imdi+xml, because crawling is done on transformed CMDI to IMDI
                                    if not version_file_missing:
                                        md_tree = ET.parse(localpath) # parse the MD file using elementTree
                                        md_root = md_tree.getroot()
                                        schema_location = md_root.attrib['{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'] # schemaLocation needed for FOXML FORMAT_URI
                                        version_attributes = {"ID": fid, "LABEL": label, "CREATED": filetime, "MIMETYPE": mimetype, "FORMAT_URI": schema_location}
                                        version_element = ET.Element('{info:fedora/fedora-system:def/foxml#}datastreamVersion', attrib=version_attributes)
                                        md_datastream.insert(0, version_element)
                                        subelement_xpath = ".//{info:fedora/fedora-system:def/foxml#}datastreamVersion[@ID='" + fid + "']"
                                        md_datastream_version = fox_root.find(subelement_xpath)
                                        ET.SubElement(md_datastream_version, '{info:fedora/fedora-system:def/foxml#}xmlContent')
                                        subelement_xpath = ".//{info:fedora/fedora-system:def/foxml#}datastreamVersion[@ID='" + fid + "']/{info:fedora/fedora-system:def/foxml#}xmlContent"
                                        md_datastream_version_content = fox_root.find(subelement_xpath)
                                        md_datastream_version_content.append(md_root)
                                    else:
                                        LOGGER.error("Could not add metadata version to FOXML because version file is missing")
                                    #md_datastream_output = ET.tostring(obj_datastream, encoding='utf8')
                                    #print("datastream output: %s" % md_datastream_output)
                                else: # versioned OBJ
                                    obj_datastream = fox_root.find(".//{info:fedora/fedora-system:def/foxml#}datastream[@ID='OBJ']") # find the OBJ datastream in the FOXML
                                    fid = "OBJ." + str(fedora_version_number)
                                    version_attributes = {"ID": fid, "LABEL": label, "CREATED": filetime, "MIMETYPE": mimetype}
                                    version_element = ET.Element('{info:fedora/fedora-system:def/foxml#}datastreamVersion', attrib=version_attributes)
                                    obj_datastream.insert(0, version_element)
                                    subelement_xpath = ".//{info:fedora/fedora-system:def/foxml#}datastreamVersion[@ID='" + fid + "']"
                                    obj_datastream_version = fox_root.find(subelement_xpath)
                                    if not checksum: # compute checksum in case it wasn't present in the database
                                        if not version_file_missing:
                                            LOGGER.info("fetching checksum for %s", localpath)
                                            md5call = "ssh " + STORAGE_USER + "@" + STORAGE_SERVER + " '" + MD5_COMMAND + " \"" + localpath + "\"'"
                                            checksum = subprocess.call(md5call, shell=True)
                                            if checksum == "00000000000000000000000": # versity slssum returns this value in case the filesystem does not have a checksum yet for the file
                                                LOGGER.info("no filesystem checksum found for %s, will compute it", localpath)
                                                md5call = "ssh " + STORAGE_USER + "@" + STORAGE_SERVER + " '" + MD5_FALLBACK + " \"" + localpath + "\"'"
                                                checksum = subprocess.call(md5call, shell=True)
                                            checksum = str(checksum)
                                        else:
                                            LOGGER.error("no checksum in DB and file missing, can't compute checksum: %s", localpath)
                                    digest_attributes = {"TYPE": "MD5", "DIGEST": checksum}
                                    ET.SubElement(obj_datastream_version, '{info:fedora/fedora-system:def/foxml#}contentDigest', attrib=digest_attributes)
                                    location_attributes = {"TYPE": "URL", "REF": localpath_with_prefix}
                                    ET.SubElement(obj_datastream_version, '{info:fedora/fedora-system:def/foxml#}contentLocation', attrib=location_attributes)
                                    obj_datastream_output = ET.tostring(obj_datastream, encoding='utf8')
                                    #print("datastream output: %s" % obj_datastream_output)
                            else:
                                ERROR_STATUS = "nocorpusnode"
                                LOGGER.error("no corpusnodes entry found for node: %s", versionnumber)
                        fedora_version_number += 1
                    # write modified XML to FOXML file in target dir
                    if ERROR_STATUS == "":
                        output_file = fox_path.replace(FOX_DIR, TARGET_FOX_DIR)
                        output_dir = os.path.dirname(output_file)
                        if not os.path.exists(output_dir):
                            os.makedirs(output_dir)
                        with open(output_file, "wb") as f:
                            f.write(ET.tostring(fox_tree, pretty_print=True, xml_declaration=True, encoding='UTF-8'))
                        LOGGER.info("versions added to %s", output_file)
            else:
                LOGGER.warning("No current PID found for version sequence %s. Deleted object?", versions_string)
    ROW += 1
CUR.close()
CONN.close()

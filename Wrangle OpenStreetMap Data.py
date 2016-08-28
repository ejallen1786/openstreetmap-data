
# coding: utf-8

# # San Francisco Data Wrangling
# 
# For this project, I will be wrangling San Francisco data from OpenStreetMap.org. I've downloaded a 59.7Mb OSM XML file of San Francisco data via Map Zen. I will be auditing and cleaning this dataset, converting it from XML to CSV format and then importing the .csv file into a SQL database. After this, I will explore the data i've imported.
# 
# Throughout this process, I will document:
# * Problems encountered in my map 
# * Overview of the data
# * Other ideas about the datasets
# 
# Map Area -- San Francisco, CA -- https://www.openstreetmap.org/relation/111968#map=10/37.8120/-122.6953

# # Taking a Look at a Data Sample 

# In[7]:

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow
from collections import defaultdict
import csv
import codecs
import re
import cerberus
import schema
import pprint


# In[3]:

SAMPLE = "/Users/elizabethallen/Documents/Udacity_P3_Project/sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    if element.tag == 'node':
        node_attribs['id'] = element.get('id')
        node_attribs['user'] = element.get('user')
        node_attribs['uid'] = element.get('uid')
        node_attribs['version'] = element.get('version')
        node_attribs['lat'] = element.get('lat')
        node_attribs['lon'] = element.get('lon')
        node_attribs['timestamp'] = element.get('timestamp')
        node_attribs['changeset'] = element.get('changeset')
        if element.iter("tag") != None:
            for tag in element.iter("tag"):
                tag_info = {}
                tag_info['id'] = element.get('id')
                tag_info['value'] = tag.attrib['v']
                m = PROBLEMCHARS.search(tag.attrib['k'])
                l = LOWER_COLON.search(tag.attrib['k'])
                if m:
                    continue
                elif l:
                    word = tag.get('k')
                    #print word
                    index = word.find(':')
                    #print index
                    tag_info['type'] = word[:index]
                    tag_info['key'] = word[index+1:]
                    #print tag_info['type']
                    #print tag_info['key']
                else:
                    tag_info['key'] = tag.attrib['k']
                    tag_info['type'] = 'regular'
                #print tag_info
                tags.append(tag_info)
            #print tags
            #node_attribs['node_tags'] = tags
            
        print node_attribs
        print tags
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        way_attribs['id'] = element.get('id')
        way_attribs['user'] = element.get('user')
        way_attribs['uid'] = element.get('uid')
        way_attribs['version'] = element.get('version')
        way_attribs['timestamp'] = element.get('timestamp')
        way_attribs['changeset'] = element.get('changeset')
        if element.iter("tag") != None:
            for tag in element.iter("tag"):
                tag_info = {}
                tag_info['id'] = element.get('id')
                tag_info['value'] = tag.attrib['v']
                m = PROBLEMCHARS.search(tag.attrib['k'])
                l = LOWER_COLON.search(tag.attrib['k'])
                if m:
                    continue
                elif l:
                    word = tag.get('k')
                    #print word
                    index = word.find(':')
                    #print index
                    tag_info['type'] = word[:index]
                    tag_info['key'] = word[index+1:]
                    #print tag_info['type']
                    #print tag_info['key']
                else:
                    tag_info['key'] = tag.attrib['k']
                    tag_info['type'] = 'regular'
                #print tag_info
                tags.append(tag_info)
            #print tags
            #node_attribs['node_tags'] = tags
        if element.iter("nd") != None:
            index = 0
            for nd in element.iter("nd"):
                tag_info = {}
                tag_info['id'] = element.get('id')
                tag_info['node_id'] = nd.attrib['ref']
                tag_info['position'] = index
                index += 1
                
                #print tag_info
                way_nodes.append(tag_info)
            #print tags
            #node_attribs['node_tags'] = tags
            
        print way_attribs
        print way_nodes
        print tags
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_strings = (
            "{0}: {1}".format(k, v if isinstance(v, str) else ", ".join(v))
            for k, v in errors.iteritems()
        )
        raise cerberus.ValidationError(
            message_string.format(field, "\n".join(error_strings))
        )


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(SAMPLE, validate=True)


# When I investigated a sample of the data using the provided systematic sampling code, i found the following issues:
# * There appears to be some second level k tags that refer to tree types
# * Street names in second level “k” tags pulled from Tiger GPS data and divided into segments, in the following format: <tag k="tiger:county" v="Alameda, CA" />
# * For second level "k" tags from Tiger GPS, the state information is included in the county field <tag k="tiger:county" v="Marin, CA" />
# * There are "relation" elements that have second level "k" tags with address abbreviations and problem characters <tag k="to" v="Sutter St &amp; Sansome St" />
# 
# Now I would like to audit the full dataset to make sure I'm not missing issues before shaping the elements for the csv file.

# # Data Audit

# In[4]:

OSMFILE = "/Users/elizabethallen/Documents/Udacity_P3_Project/san-francisco_california.osm"
EXAMPLE = "/Users/elizabethallen/Documents/Udacity_P3_Project/sample.osm"


# First, I'm going to iteratively parse the xml file to find what elements there are and what their attributes look like. For this and all iterative functions, I will test on the EXAMPLE file first and then execute on the OSMFILE.
# 
# I expect the output to look something like: 
# {"bounds": {"count": 1,
#             "attributes": ["attrib1", "attrib2",...]},
#  "tag": {{}}}

# In[5]:

def count_tags(filename):
    # Parse the xml file iteratively and add new tag types and attributes to a dictionary and for existing tags and
    # attributes, increment the count by 1. Return the dictionary of tag types, attribute types and their counts. 
    tag_types = {}
    
    for event, elem in ET.iterparse(filename):
        tag_info = {}
        tag = elem.tag
        if tag in tag_types:
            tag_types[tag]['count'] = tag_types[tag]['count'] + 1
            for attrib, val in elem.attrib.items():
                if attrib in tag_types[tag]['attributes']:
                    continue
                else:
                    tag_types[tag]['attributes'].append(attrib)
        else:
            tag_info['count'] = 1
            attrib_info = []
            for attrib, val in elem.attrib.items():
                attrib_info.append(attrib)
            tag_info['attributes'] = attrib_info
            tag_types[tag] = tag_info
    return tag_types

def test():

    tags = count_tags(OSMFILE)
    pprint.pprint(tags)

if __name__ == "__main__":
    test()


# Now I want to explore further and audit for problem characters, colons and other potential issues. I want to group them by tag and attribute type. Again, I'll test on the sample osm first, refine based on the output and then run it on the full osm file.
# 
# I expect the output to look like this:
# {"bounds": {attrib1: {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0},
#             attrib2: {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}},
#  "tag": {}}

# In[6]:

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
alldigits = re.compile(r'^[0-9]+$')
allletters = re.compile(r'^[a-zA-Z]+$')

def key_audit(attrib, val, problems):
    # Take the attribute, its associated value, and a dictionary of problem types as input. Search each attribute's
    # associated value for matches to the regex search conditions outlined above. For each match, increment the
    # associated problem type by 1. Return the dictionary of problems.
    m = lower.search(val)
    l = lower_colon.search(val)
    p = problemchars.search(val)
    a = alldigits.search(val)
    x = allletters.search(val)
    if m:
        lower_case = m.group()
        problems["lower"] += 1
    elif l:
        colon = l.group()
        problems["lower_colon"] += 1
    elif p:
        problem = p.group()
        problems["problemchars"] += 1
    elif a:
        digits = a.group()
        problems["alldigits"] += 1
    elif x:
        letters = x.group()
        problems['allletters'] += 1
    else:
        problems["other"] += 1
    return problems
   
def process_map(filename):
    # Iteratively parse the OSM file and for each element, look at the tag and see if it already exists in the tag_types
    # dictionary. If it does not, add it to the dictionary and evaluate its attributes for problems via the key_audit 
    # function. If it does exist, audit its attribute values viea the key_audit and increment any existing problems by 1.
    # return the dictionary of tag_types.
    tag_types = {}
    
    for _, element in ET.iterparse(filename):
        tag = element.tag
        if tag in tag_types:
            for attrib, val in element.attrib.items():
                attrib_info = {}
                if attrib in tag_types[tag]:
                    problems = tag_types[tag][attrib]
                    tag_types[tag][attrib] = key_audit(attrib, val, problems)
                else:
                    attrib_info = tag_types[tag]
                    problems = {"lower": 0, "lower_colon": 0, "problemchars": 0, "alldigits": 0, "allletters": 0, "other": 0}
                    attrib_info[attrib] = key_audit(attrib, val, problems)
                    tag_types[tag] = attrib_info
        else:
            attrib_info = {}
            for attrib, val in element.attrib.items():
                problems = {"lower": 0, "lower_colon": 0, "problemchars": 0, "alldigits": 0, "allletters": 0, "other": 0}
                attrib_info[attrib] = key_audit(attrib, val, problems)
            tag_types[tag] = attrib_info
    return tag_types

def test():
    element_info = process_map(OSMFILE)
    pprint.pprint(element_info)

if __name__ == "__main__":
    test()


# I see the following potential issues:
# * Lots of problem characters
# * Seems as though lat and lon values have problem characters due to the presence of a '.', but I should confirm
# * Every tag has attribute values with a mix of colons and problem characters
#     
# For the most part, the data in this set actually looks pretty nice!
# 
# From the exploration so far, I know what sort of elements are present and what sort of attributes are contained within. Additionally, I can see where I need to focus further auditing. I can also make some choices around which elements I want in my CSV and what the schema should contain.
# 
# Before I move onto importing my data into csv files, I want to audit in more detail to see where I can make some improvements to the data. I definitely want to check out street names, based on my exercise during the lessons. Additionally, from looking at the small sample file, I noticed that the tag element also contains state, country, postcode and even phone numbers in some cases. I want to run a similar audit on these elements.
# 
# I'm going to check the following:
# * The street names to see if what types of streets the file contains and find any cases of abbreviations so that I can correct them via a mapping
# * The state is always set to CA
# * The country is always set to US
# * The postcode is numeric and of the format XXXXX or XXXXX-XXXX
# * The phone numbers follow NANP standards (https://en.wikipedia.org/wiki/National_conventions_for_writing_telephone_numbers)

# In[8]:

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]

def audit_street_type(street_types, street_name):
    # Takes a dictionary of street types and the street name, searches the name for a match to the regular expression
    # and if a match is found, checks if it's in the list of expected streets, and if not, adds the street type to the
    # dictionary.
    
    m = street_type_re.search(street_name)
     
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    # Checks the element passed to it to see if it refers to a street address and returns True if it does
    return (elem.attrib['k'] == "addr:street")

def audit(osmfile):
    # Parses the osm file provided, checks if the element tag is equal to node or way, and if it is, iterates over
    # the child tags to see if they contain street address information. If it does, it audits the street name.
    # Returns the street_type dictionary.
    
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])    
    print street_types.keys()
    osm_file.close()
    return street_types

if __name__ == '__main__':
    audit(OSMFILE)


# From the small sample that I looked through earlier, I didn't see any instances of abbreviation. From running this street name audit script on the full OSM file, I can see a few cases of abbreviation that we will want to fix when adding this data to the CSV.
# 
# Now I'm going to move on to an audit of states, postal codes, countries and phone numbers.

# In[9]:

lower_colon_re = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
postal_code_re = re.compile(r'\d{5}(\-\d{4}$)?')
phone_number_re = re.compile(r'(\d\-)?\d{3}\-\d{3}\-\d{4}|\(\d{3}\)\s\d{3}\-\d{4}|\d{3}\.\d{3}\.\d{4}')
california_re = re.compile(r'[C|c][A|z]')
unitedstates_re = re.compile(r'[U|u][S|s]')

def audit(audit_vals, tag_type, tag_val):
    # Takes a dictionary of audit values, the tag type and the tag value, searches the tag value for state, country
    # postal code or phone number matches, and based on the type of tag, increments the audit matches. If it is not 
    # a match, Other is incremented. Returns the audit values dictionary.
    c = california_re.search(tag_val)
    u = unitedstates_re.search(tag_val)
    l = postal_code_re.search(tag_val)
    p = phone_number_re.search(tag_val)
    if tag_type == 'addr:state':
        if c:
            california = c.group()
            audit_vals['california']['Match'] += 1
        else:
            audit_vals['california']['Other'] += 1
    elif 'country' in tag_type:
        if u:
            unitedstates = u.group()
            audit_vals['unitedstates']['Match'] += 1
        else:
            audit_vals['unitedstates']['Other'] += 1
    elif 'postcode' in tag_type:
        if l:
            postcode = l.group()
            audit_vals['postcodes']['Match'] += 1
        else:
            audit_vals['postcodes']['Other'] += 1
    elif 'phone' in tag_type:
        if p:
            phonenumbers = p.group()
            audit_vals['phonenumbers']['Match'] += 1
        else:
            audit_vals['phonenumbers']['Other'] += 1
    
    return audit_vals

def process_map(filename):
    # Iteratively parses the OSM file and sees if the tags match node or way. If they do, iterates over the child
    # tags, grabs their attribute keys and values and then audits them. Returns the ditionary of audit values.
    audit_vals = {'california': {'Match': 0, 'Other': 0},
             'postcodes': {'Match': 0, 'Other': 0},
             'phonenumbers': {'Match': 0, 'Other': 0},
             'unitedstates': {'Match': 0, 'Other': 0}
            }
    
    for _, element in ET.iterparse(filename):
        if element.tag == "node" or element.tag == "way":
            for tag in element.iter("tag"):
                tag_type = tag.attrib['k']
                tag_val = tag.attrib['v']
                audit_vals = audit(audit_vals, tag_type, tag_val)
                
    return audit_vals   
        
def test():
    element_info = process_map(OSMFILE)
    pprint.pprint(element_info)

if __name__ == "__main__":
    test()


# I'm pleasanty surprised by how many "other" values I found when compared to the number of matches. To understand how I should best approach the "other" values, I want to look at each of them (since they aren't massively high numbers) and see what sort of problems we see that prevent the match.

# In[10]:

def audit(other_vals, tag_type, tag_val):
    # Takes a set of audit values, the tag type and the tag value, searches the tag value for state, country
    # postal code or phone number matches. If no match is found, the tag value is added to the set for the tag. 
    # Returns the set of values.
    c = california_re.search(tag_val)
    u = unitedstates_re.search(tag_val)
    l = postal_code_re.search(tag_val)
    p = phone_number_re.search(tag_val)
    if tag_type == 'addr:state':
        if not c:
            other_vals[tag_type].add(tag_val)
    elif 'country' in tag_type:
        if not u:
            other_vals[tag_type].add(tag_val)
    elif 'postcode' in tag_type:
        if not l:
            other_vals[tag_type].add(tag_val)
    elif 'phone' in tag_type:
        if not p:
            other_vals[tag_type].add(tag_val)
    
    return other_vals

def process_map(filename):
    # Iteratively parses the OSM file and sees if the tags match node or way. If they do, iterates over the child
    # tags, grabs their attribute keys and values and then audits them for non-matching values. 
    # Returns the set of non-matching values.
    other_vals = defaultdict(set)
    for event, elem in ET.iterparse(filename, events=("start",)):
        
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                tag_type = tag.attrib['k']
                tag_val = tag.attrib['v']
                other_vals = audit(other_vals, tag_type, tag_val)    
    print other_vals
    return other_vals

if __name__ == '__main__':
    process_map(OSMFILE)


# Based on this output, there are definitely a few action items:
# * There are some records with non-US countries -- I want to keep them for now to investigate further during my SQL analysis 
# * Because there are so many variations on phone number presentation, I will simplify the audit for the purpose of this exercise and simply ignore phone numbers that contain letters or numbers that contain more than 11 digits
# * I should consider adjusting my state regex to capture various capitalization and spelling out of California and ignore all other values
# * I will ignore postcodes that do not match my regex
# 
# Note that I could take this a step further and do the below, but for the simplicity of this exercise, I will not do so right now.
# * Investigate the international country records to see if there's any obvious information leading me to believe that it was a typo
# * Translate letters in phone numbers to their associated numbers

# # Data Shaping
# 
# Now that I've completed some basic auditing, I want to prepare the data to be inserted into a SQL database.
# To do so, I'm going to parse the elements in the OSM XML file, transforming them from document format to
# tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
# imported to a SQL database as tables.
# 
# The process for this transformation is as follows:
# * Use iterparse to iteratively step through each top level element in the XML
# * Shape each element into several data structures using a custom function
# * Utilize a schema and validation library to ensure the transformed sample data is in the correct format
# * Write each data structure to the appropriate .csv files
# 
# Below will be the code needed to load the data, perform iterative parsing and write the
# output to csv files. I will also have a shape_element function that will transform each
# element into the correct format. I already have a defined a schema in the schema.py file for the .csv files and the eventual tables (schema provided below). Using the cerberus library I can validate the output of the sample data against this schema to ensure it is correct. When I execute this against the full dataset, I will not validate to avoid the increase in processing time.

# In[11]:

# Here is the schema I want:

NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
# Top level node attributes
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
# id is the top level node id, key is the "k" attribute of the tag if no colon is present or the characters after the
# colon if one is, value is the "v" attribute of the tag, and type is
# either the characters before the colon in the "k" attribute or "regular" if no colon is present
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
# Top level way attributes
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
# id is the top level node id, key is the "k" attribute of the tag if no colon is present or the characters after the
# colon if one is, value is the "v" attribute of the tag, and type is
# either the characters before the colon in the "k" attribute or "regular" if no colon is present
WAY_NODES_FIELDS = ['id', 'node_id', 'position']
# id is the top level way id, node_id is the ref attribute of the nd tag, and position is the index of the nd tag

# I'm choosing to ignore osm, member, bounds and relation elements


# In[15]:

# Based on the earlier analysis of street types, I have updated the mappings dictionary for street types

street_mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Rd": "Road",
            "Rd.": "Road",
            "Dr.": "Drive",
            "Dr": "Drive",
            "Pl": "Place",
            "Plz": "Plaza",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "Ct": "Court",
            "Ctr": "Center",
            "Ln.": "Lane"
            }


# In[13]:

# Here is the schema that I will test a sample of the OSM file on. Note that I will not test the full OSM file against
# this schema due to performance implications.
SCHEMA = schema.schema


# Just a quick note: if I wanted to be more thorough in this audit, I would also include bound, relation and member elements in this schema. I chose to ignore those elements for simplicity.

# The "node" field should hold a dictionary of the following top level node attributes:
# - id
# - user
# - uid
# - version
# - lat
# - lon
# - timestamp
# - changeset
# 
# All other attributes can be ignored
# 
# The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
# child tags of node which have the tag name/type: "tag". Each dictionary should have the following
# fields from the secondary tag attributes:
# - id: the top level node id attribute value
# - key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
# - value: the tag "v" attribute value
# - type: either the characters before the colon in the tag "k" value or "regular" if a colon is not present.
# 
# Additionally,
# 
# - if the tag "k" value contains problematic characters, the tag should be ignored
# - if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
#   and characters after the ":" should be set as the tag key
# - if there are additional ":" in the "k" value they and they should be ignored and kept as part of
#   the tag key. For example:
# 
#   <tag k="addr:street:name" v="Lincoln"/>
#   should be turned into
#   {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}
# 
# - If a "k" value contains "phone", ignore phone numbers that contain letters or numbers that contain more than 11 digits
# - If a "k" value contains "state", update variations of "CA" to be consistent with my state mapping and other values should be set to None
# - If a "k" value contains "postcode", set any values that do not match a standard Zipcode format to None
# - If a "k" value contains "street", update the names to spell out the street type abbreviations based on my mapping
# - If a node has no secondary tags then the "node_tags" field should just contain an empty list.
# 
# The final return value for a "node" element should look something like:
# 
# {'node': {'id': 757860928,
#           'user': 'uboot',
#           'uid': 26299,
#        'version': '2',
#           'lat': 41.9747374,
#           'lon': -87.6920102,
#           'timestamp': '2010-07-22T16:16:51Z',
#       'changeset': 5288876},
#  'node_tags': [{'id': 757860928,
#                 'key': 'amenity',
#                 'value': 'fast_food',
#                 'type': 'regular'},
#                {'id': 757860928,
#                 'key': 'cuisine',
#                 'value': 'sausage',
#                 'type': 'regular'},
#                {'id': 757860928,
#                 'key': 'name',
#                 'value': "Shelly's Tasty Freeze",
#                 'type': 'regular'}]}
# 
# If the element top level tag is "way":
# The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}
# 
# The "way" field should hold a dictionary of the following top level way attributes:
# - id
# - user
# - uid
# - version
# - timestamp
# - changeset
# 
# All other attributes can be ignored
# 
# The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
# for "node_tags".
# 
# Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
# dictionaries, one for each nd child tag.  Each dictionary should have the fields:
# - id: the top level element (way) id
# - node_id: the ref attribute value of the nd tag
# - position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within the way element
# 
# The final return value for a "way" element should look something like:
# 
# {'way': {'id': 209809850,
#          'user': 'chicago-buildings',
#          'uid': 674454,
#          'version': '1',
#          'timestamp': '2013-03-13T15:58:04Z',
#          'changeset': 15353317},
#  'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
#                {'id': 209809850, 'node_id': 2199822390, 'position': 1},
#                {'id': 209809850, 'node_id': 2199822392, 'position': 2},
#                {'id': 209809850, 'node_id': 2199822369, 'position': 3},
#                {'id': 209809850, 'node_id': 2199822370, 'position': 4},
#                {'id': 209809850, 'node_id': 2199822284, 'position': 5},
#                {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
#  'way_tags': [{'id': 209809850,
#                'key': 'housenumber',
#                'type': 'addr',
#                'value': '1412'},
#               {'id': 209809850,
#                'key': 'street',
#                'type': 'addr',
#                'value': 'West Lexington St.'},
#               {'id': 209809850,
#                'key': 'street:name',
#                'type': 'addr',
#                'value': 'Lexington'},
#               {'id': '209809850',
#                'key': 'street:prefix',
#                'type': 'addr',
#                'value': 'West'},
#               {'id': 209809850,
#                'key': 'street:type',
#                'type': 'addr',
#                'value': 'Street'},
#               {'id': 209809850,
#                'key': 'building',
#                'type': 'regular',
#                'value': 'yes'},
#               {'id': 209809850,
#                'key': 'levels',
#                'type': 'building',
#                'value': '1'},
#               {'id': 209809850,
#                'key': 'building_id',
#                'type': 'chicago',
#                'value': '366409'}]}

# In[18]:

street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
lower_colon_re = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
postal_code_re = re.compile(r'\d{5}(\-\d{4}$)?')
phone_number_re = re.compile(r'(\d\-)?\d{3}\-\d{3}\-\d{4}|\(\d{3}\)\s\d{3}\-\d{4}|\d{3}\.\d{3}\.\d{4}')
california_re = re.compile(r'[C|c][A|a]([L|l][I|i][F|f][O|o][R|r][N|n][I|i][A|a])?')
unitedstates_re = re.compile(r'[U|u][S|s]')
contains_letters_re = re.compile('[a-zA-Z]')

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    # Takes the element and fields lists and shapes the element into python dictionaries according to the rules 
    # I outlined above. Returns the dictionaries.
    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements
    
    if element.tag == 'node':
        
        node_attribs = get_attribs(node_attribs, element)
        if skip_record(NODE_FIELDS, node_attribs):
            return False
        if element.iter("tag") != None:
            tags = get_tag_info(element, tags)
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
                
        way_attribs = get_attribs(way_attribs, element)
        if skip_record(WAY_FIELDS, way_attribs):
            return False
        
        if element.iter("tag") != None:
            tags = get_tag_info(element, tags)
        if element.iter("nd") != None:
            index = 0
            for nd in element.iter("nd"):
                tag_info = {}
                tag_info['id'] = element.get('id')
                tag_info['node_id'] = nd.attrib['ref']
                tag_info['position'] = index
                index += 1
                
                way_nodes.append(tag_info)
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

def skip_record(FIELDS, attribs):
    # Takes the list of fields expected and the dictionary of attributes from the element and checks if the attributes
    # are in the list of Fields or if the value associated with the attribute is empty. If the attribute is not in the
    # list of fields or if the value of the attribute is empty, returns True.
    for val in FIELDS:
        if val not in attribs:
            return True
        elif attribs[val] == 'NULL' or attribs[val] == None or attribs[val] == '': 
            return True
    

def get_attribs(attrib_dict, element):
    # Takes attribute dictionary and element, and for each attribute in the element, it adds the attribute name
    # to the attribute dictionary and returns the dictionary.
    for attrib in element.attrib:
        attrib_dict[attrib] = element.get(attrib)
    
    return attrib_dict
    

def get_value(word, search_val):
    # Takes an attribute and its value and does a regex search on the value depending on the type of attribute. 
    # Depending on whether a match is found or not, an associated value is returned.
    c = california_re.search(search_val)
    l = postal_code_re.search(search_val)
    p = phone_number_re.search(search_val)
    s = street_type_re.search(search_val)
    a = contains_letters_re.search(search_val)
    if word == 'addr:state':
        if c:
            california = c.group()
            value = 'CA'
        else:
            value = 'None'
    elif word == 'addr:street':
        if s:
            street_type = s.group()
            value = update_name(search_val, street_mapping)
        else:
            value = search_val
    elif 'postcode' in word:
        if l:
            postcode = l.group()
            value = postcode
        else:
            value = 'None'
    elif 'phone' in word:
        if a:
            value = 'None'
        else:
            value = search_val
    else:
        value = search_val
    return value

def update_name(name, street_mapping):
    # Takes the a name and updates the name to include the approved name mapping. Returns the new name. 
    for key in street_mapping:
        if name.find(street_mapping[key]) == -1:
            if name.find(key) != -1:
                name = name.replace(key, street_mapping[key])
    return name

def get_tag_info(element, tags):
    # Takes the element and empty list, runs a regex search for problem characters on the tag and colons and 
    # returns the list with the clean tag info.
    for tag in element.iter("tag"):
        tag_info = {}
        tag_info['id'] = element.get('id')
        word = tag.get('k')
        value = tag.get('v')
    
        m = PROBLEMCHARS.search(tag.attrib['k'])
        l = LOWER_COLON.search(tag.attrib['k'])
        if m:
            continue
        elif l:
            index = word.find(':')
            tag_info['type'] = word[:index]
            tag_info['key'] = word[index+1:]
            tag_info['value'] = get_value(word, tag.attrib['v'])  
        else:
            tag_info['key'] = tag.attrib['k']
            tag_info['type'] = 'regular'
            tag_info['value'] = get_value(word, tag.attrib['v'])
        tags.append(tag_info)
    return tags

# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_strings = (
            "{0}: {1}".format(k, v if isinstance(v, str) else ", ".join(v))
            for k, v in errors.iteritems()
        )
        raise cerberus.ValidationError(
            message_string.format(field, "\n".join(error_strings))
        )


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                '''if validate is True:
                    validate_element(el, validator)'''

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSMFILE, validate=True)


# When I first ran the above code, the validator gave off errors showing that I have at least one case where a uid or user attribute or value is missing for node tags. To avoid this issue, I'm going to ignore cases where node or way attributes or attribute values are missing. I adjusted the above code to achieve this.
# 
# Additionally, since validation is ~10x slower, I'm going to remove code calling the validator.
# 
# Now that the code is working as expected, here are the file sizes:
# * ways_nodes.csv -- 128.8MB
# * ways_tags.csv -- 48.1MB
# * ways.csv -- 30.9MB
# * nodes_tags.csv -- 8.6MB
# * nodes.csv -- 378.9MB
# 
# Now that I have my csv files ready, I want to prep my database. I created a db using the following terminal command:
# sqlite3 SanFrancisco.db
# 
# Now I want to create a set of tables!

# # Data Import
# 
# There are a few issues I ran into while trying to create tables and import the CSVs into those tables:
# * I initially tried to import them via terminal queries outlined below, but i had a bunch of insert failures due to UNIQUE CONSTRAINTS. This was due the fact that I created primary keys that should've been just foreign keys in the nodes_tags, ways_nodes and ways_tags tables. I also saw this surface as an issue when I tried to do follow-up SQL queries to the tables and found many rows missing. I adjusted the create table queries to fix for this.
# * I initially tried to import them via terminal queries outlined below, but i had a a few datatype mismatch failures, which I believe was due to my header row in the ways and nodes tables. I believe it was due to header rows because when I did follow-up SQL queries, I could'nt ask for specic fields from the ways and nodes tables. I adjusted this in my follow-up programmatic table creation queries below to use a decode utf method, which seems to fix it.
# 
# Here are the original import my CSVs queries I issued to terminal sqlite3:
# 
# * sqlite> .mode csv
# * sqlite> .import /Users/elizabethallen/Documents/Udacity_P3_Project/ways_nodes.csv ways_nodes
# * sqlite> .import /Users/elizabethallen/Documents/Udacity_P3_Project/ways_tags.csv ways_tags
# * sqlite> .import /Users/elizabethallen/Documents/Udacity_P3_Project/ways.csv ways
# * sqlite> .import /Users/elizabethallen/Documents/Udacity_P3_Project/nodes_tags.csv nodes_tags
# * sqlite> .import /Users/elizabethallen/Documents/Udacity_P3_Project/nodes.csv nodes
# 
# Now here are the adjusted queries I used to programmitcally create tables and insert CSVs into the tables.

# In[22]:

# I decided to create the tables directly using terminal, but i could've created a python script executing the 
# following queries:

NODES_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
'''NODES_DECODED = (i['id'].decode("utf-8"), i['lat'].decode("utf-8"), i['lon'].decode("utf-8"), i['user'].decode("utf-8"),
                i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"),
                i['timestamp'].decode("utf-8"))'''
NODES_INSERT = "INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"
NODES_QUERY = '''CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    lat FLOAT,
    lon FLOAT,
    user STRING,
    uid INTEGER,
    version STRING,
    changeset INTEGER,
    timestamp STRING
    );
'''

NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
#NODES_TAGS_DECODED = (i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), i['type'].decode("utf-8"))
NODES_TAGS_INSERT = '''INSERT INTO nodes_tags(id, key, value, type) VALUES (?, ?, ?, ?);'''
NODES_TAGS_QUERY = '''CREATE TABLE nodes_tags (
    id INTEGER,
    key STRING,
    value STRING,
    type STRING,
    FOREIGN KEY (id) REFERENCES nodes
    );'''

WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
'''WAYS_DECODED = (i['id'].decode("utf-8"), i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
                i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8"))'''
WAYS_INSERT = '''INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);'''
WAYS_QUERY = '''CREATE TABLE ways (
    id INTEGER PRIMARY KEY,
    user STRING,
    uid INTEGER,
    version STRING,
    changeset INTEGER,
    timestamp STRING
    );'''

WAY_NODES_FIELDS = ['id', 'node_id', 'position']
#WAYS_NODES_DECODED = (i['id'].decode("utf-8"), i['node_id'].decode("utf-8"), i['position'].decode("utf-8"))
WAYS_NODES_INSERT = '''INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);'''
WAYS_NODES_QUERY = '''CREATE TABLE ways_nodes (
    id INTEGER,
    node_id INTEGER,
    position INTEGER,
    FOREIGN KEY (id) REFERENCES ways,
    FOREIGN KEY (id) REFERENCES ways_tags
    );'''

WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
#WAYS_TAGS_DECODED = (i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), i['type'].decode("utf-8"))
WAYS_TAGS_INSERT = '''INSERT INTO ways_tags(id, key, value, type) VALUES (?, ?, ?, ?);'''
WAYS_TAGS_QUERY = '''CREATE TABLE ways_tags (
    id INTEGER,
    key STRING,
    value STRING,
    type STRING,
    FOREIGN KEY (id) REFERENCES ways,
    FOREIGN KEY (id) REFERENCES ways_nodes
    );'''


# In[23]:

import sqlite3
import csv
from pprint import pprint


# In[24]:

filename = "/Users/elizabethallen/Documents/Udacity_P3_Project/SanFrancisco.db"
nodes = '/Users/elizabethallen/Documents/Udacity_P3_Project/nodes.csv'
nodes_tags = '/Users/elizabethallen/Documents/Udacity_P3_Project/nodes_tags.csv'
ways = '/Users/elizabethallen/Documents/Udacity_P3_Project/ways.csv'
ways_tags = '/Users/elizabethallen/Documents/Udacity_P3_Project/ways_tags.csv'
ways_nodes = '/Users/elizabethallen/Documents/Udacity_P3_Project/ways_nodes.csv'


# In[35]:

conn = sqlite3.connect(filename)
cur = conn.cursor()
cur.execute('''DROP TABLE ways_tags;''')
conn.commit()


# In[27]:

# Create nodes table and import data from csv

conn = sqlite3.connect(filename)
cur = conn.cursor()

# Create the table, specifying the column names and data types:
cur.execute(NODES_QUERY)
conn.commit()

# Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open(nodes,'rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"), i['lon'].decode("utf-8"), i['user'].decode("utf-8"),
                i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"),
                i['timestamp'].decode("utf-8")) for i in dr]
    #print to_db

# insert the formatted data
cur.executemany(NODES_INSERT, to_db)
# commit the changes
conn.commit()

# Now I want to check a subset of my data!
cur.execute('SELECT id FROM nodes LIMIT 10')
all_rows = cur.fetchall()
print('1):')
pprint(all_rows)


# In[30]:

# Create nodes_tags table and import data from csv
conn = sqlite3.connect(filename)
cur = conn.cursor()

cur.execute(NODES_TAGS_QUERY)
conn.commit()

with open(nodes_tags,'rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), 
              i['type'].decode("utf-8")) for i in dr]

cur.executemany(NODES_TAGS_INSERT, to_db)
conn.commit()

cur.execute('SELECT id FROM nodes_tags LIMIT 10')
all_rows = cur.fetchall()
print('1):')
pprint(all_rows)


# In[32]:

# Create ways table and import data from csv
conn = sqlite3.connect(filename)
cur = conn.cursor()

cur.execute(WAYS_QUERY)
conn.commit()

with open(ways,'rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
                i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]

cur.executemany(WAYS_INSERT, to_db)
conn.commit()

cur.execute('SELECT id FROM ways LIMIT 10')
all_rows = cur.fetchall()
print('1):')
pprint(all_rows)


# In[34]:

# Create ways_nodes table and import data from csv
conn = sqlite3.connect(filename)
cur = conn.cursor()

cur.execute(WAYS_NODES_QUERY)
conn.commit()

with open(ways_nodes,'rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"), i['position'].decode("utf-8")) for i in dr]

cur.executemany(WAYS_NODES_INSERT, to_db)
conn.commit()

cur.execute('SELECT id FROM ways_nodes LIMIT 10')
all_rows = cur.fetchall()
print('1):')
pprint(all_rows)


# In[36]:

# Create ways_tags table and import data from csv
conn = sqlite3.connect(filename)
cur = conn.cursor()

cur.execute(WAYS_TAGS_QUERY)
conn.commit()

with open(ways_tags,'rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"), i['value'].decode("utf-8"), 
              i['type'].decode("utf-8")) for i in dr]

cur.executemany(WAYS_TAGS_INSERT, to_db)
conn.commit()

cur.execute('SELECT id FROM ways_tags LIMIT 10')
all_rows = cur.fetchall()
print('1):')
pprint(all_rows)


# In[37]:

conn.close()


# Now that I have my tables and data imported into SQLite, I want to see how many rows each table has using the below query for each table.

# In[38]:

filename = "/Users/elizabethallen/Documents/Udacity_P3_Project/SanFrancisco.db"


# In[44]:

def row_count(filename, QUERY):
    # Takes database file and count query and returns output of SQL query
    db = sqlite3.connect(filename)
    c = db.cursor()
    c.execute(QUERY)
    results = c.fetchall()
    #print(results)
    return results

print 'nodes count: ' + str(row_count(filename, '''SELECT COUNT(*) FROM nodes;'''))
print 'nodes_tags count: ' + str(row_count(filename, '''SELECT COUNT(*) FROM nodes_tags;'''))
print 'ways count: ' + str(row_count(filename, '''SELECT COUNT(*) FROM ways;'''))
print 'ways_tags count: ' + str(row_count(filename, '''SELECT COUNT(*) FROM ways_tags;'''))
print 'ways_nodes count: ' + str(row_count(filename, '''SELECT COUNT(*) FROM ways_nodes;'''))


# # Data Exploration
# 
# Now that I have all my data in my database, I would like to take a look at the following:
# * What are the top 10 cities?
# * What are the top 10 zipcodes?
# * How many non-US records are there and what do the records look like?
# * How many non-CA records are there and what do the records look like?
# * Who are the top 10 contributors?
# * How many unique users are there?
# * What's the min, max and mean for the number of posts per user?

# In[46]:

filename = "/Users/elizabethallen/Documents/Udacity_P3_Project/SanFrancisco.db"

db = sqlite3.connect(filename)
c = db.cursor()


# In[47]:

# What are the top 10 cities?
QUERY = ''' 
SELECT key
, value
, count(*) as count
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE key LIKE '%city%'
GROUP BY 1,2
ORDER BY count DESC 
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[48]:

# What are the top 10 zipcodes?
QUERY = ''' 
SELECT key
, value
, COUNT(*)
FROM (
    SELECT *
    FROM nodes_tags
    UNION ALL
    SELECT *
    FROM ways_tags) as all_tags
WHERE key LIKE '%postcode%'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[49]:

# What do non-US records look like and how many of them are there?
QUERY = ''' 
SELECT key
, value
, COUNT(*)
FROM (
    SELECT *
    FROM nodes_tags
    UNION ALL
    SELECT *
    FROM ways_tags) as all_tags
WHERE key LIKE '%country%'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 20
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[50]:

# What do non-CA records look like and how many of them are there?
QUERY = ''' 
SELECT key
, value
, COUNT(*)
FROM (
    SELECT *
    FROM nodes_tags
    UNION ALL
    SELECT *
    FROM ways_tags) as all_tags
WHERE key LIKE '%state%'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 20
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[51]:

# Who are the top 10 contributors?
QUERY = '''
SELECT user
, COUNT(*)
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[52]:

# How many unique users are there?
QUERY = '''
SELECT COUNT(DISTINCT user)
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
;
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[53]:

# What's the min, max and mean for the number of posts per user?
QUERY = '''
SELECT min(cnt) as min
, max(cnt) as max
, avg(cnt) as avg
FROM (
SELECT user
, COUNT(*) as cnt
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
GROUP BY 1
ORDER BY 2 DESC) 
;
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# Through this initial set of queries, I do see:
# * There are some non-US records, but there's only a handful, so I will ignore them in further analysis
# * There are very few obviously non-CA state records, which is good!
# * It looks like there are a lot of records in the sunset postalcode!
# * Even though the most data-rich postalcode is in the sunset, there are more records for the Redwood City area than other cities -- more than San Francisco
# * The massive difference between the max and mean values makes me want to explore the distribution further
# 
# Before I start exploring the distribution, I want to investigate some of the keys, their values, and what types of posts are most popular.

# In[54]:

#what are the top key types?

QUERY = ''' 
SELECT key
, count(*) as count
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
GROUP BY 1
ORDER BY count DESC 
LIMIT 50
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[55]:

# What are the top 10 amenities?
QUERY = ''' 
SELECT key
, value
, count(*) as count
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE key == 'amenity'
GROUP BY 1,2
ORDER BY count DESC 
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[56]:

# What are the top 10 cuisine types for restaurants?
QUERY = ''' 
SELECT r.value
, c.value
, COUNT(*) 
FROM (
SELECT *
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE value == 'restaurant') as r,
(SELECT *
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE key == 'cuisine') as c
ON r.id = c.id
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[57]:

# What are the top 10 religions for places of worship?
QUERY = ''' 
SELECT r.value
, c.value
, COUNT(*) 
FROM (
SELECT *
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE value == 'place_of_worship') as r,
(SELECT *
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE key == 'religion') as c
ON r.id = c.id
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[58]:

# What are the top 10 shop types?
QUERY = ''' 
SELECT key
, value
, COUNT(*)
FROM (
    SELECT *
    FROM ways_tags
    UNION ALL
    SELECT *
    FROM nodes_tags) as all_tags
WHERE key == 'shop'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 10
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# In[59]:

conn.close()


# My thoughts on the key and amenity exploration:
# * Given the struggles of being a vehicle owner in SF, I'm surprised that parking is the most abundant amenity (though realistically I understand why this shouldn't be surprising)
# * I'm absolutely NOT surprised that mexican is the top restaurant cuisine ;)
# * Christian as the top place of worship is also not surprising, and given the ethnic breakdown of the population, I'm also not surprised that Buddhist is 2nd most frequent
# * Clearly we have a bit of a data issue with shops, with "yes" being one of the top 10 shop types
# 
# Unfortunately, SQLite is a bit limited in its mathematical functions, and I would like to explore the distribution of user post activity.
# 
# I'm going to approach this with a mix of SQL queries and then I'm going to create a pandas dataframe to apply some more advanced mathematical functions and maybe even do a little plotting!

# In[60]:

db = sqlite3.connect(filename)
c = db.cursor()

# How many users have posted more than the average amount?

QUERY = '''
SELECT COUNT(DISTINCT t.user)
FROM (
SELECT user
, COUNT(*) as cnt
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
    GROUP BY 1
    ORDER BY 2 DESC) as t,
(SELECT ROUND(avg(cnt),2) as avg
FROM (
SELECT user
, COUNT(*) as cnt
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
GROUP BY 1
ORDER BY 2 DESC)) as a 
WHERE t.cnt > a.avg
;
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# That means that only 4.9% of users have posted more than the average amount. Now let's see how many users have only posted 1 time.

# In[61]:

# How many users have posted only once?

QUERY = '''
SELECT COUNT(DISTINCT user)
FROM (
SELECT user
, COUNT(*) as cnt
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_users
GROUP BY 1
HAVING cnt == 1)
;
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results)


# This means that almost 24% of users have only posted 1 time. This leads me to further believe that the distribution of posts will be heavily skewed towards a handful of users.
# 
# Let's confirm this by exploring a dataframe of user posts and applying some more advanced statistics and maybe do some plotting!

# In[67]:

# I want to get a list of all users who have posted, the count of their posts, and the percentage of total posts 
# attributed to each user

QUERY = '''
SELECT user
, cnt
, ROUND(ROUND(cnt,4) / ROUND(total,4) * 100,2) as percent
FROM (
SELECT u.user as user
, u.cnt as cnt
, a.total as total
FROM (
SELECT COUNT(*) as total
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways)) as a
JOIN (
SELECT user
, COUNT(*) as cnt
FROM (
    SELECT user
    FROM nodes
    UNION ALL
    SELECT user
    FROM ways) as all_posts
GROUP BY 1
ORDER BY 2 DESC) as u
ORDER BY 2 DESC)
ORDER BY 2 DESC
;
'''

c.execute(QUERY)
results = c.fetchall()
pprint(results[0:10])


# Now that I have a list of users and their post counts, I want to create a dataframe from this. First I'll turn this data into an array.

# In[68]:

import numpy as np
import pandas as pd

array = np.array(results)
print array[0]


# In[69]:

# Now I will create a dataframe

columns = ['user', 'count', 'percent']
df = pd.DataFrame(array, columns=columns)


# In[70]:

# Let's take a look!

df.head()


# When I tried to apply some quick aggregate functions, like .mean(), I got 'inf' as the output. Let's look at the datatypes of my columns.

# In[71]:

df.dtypes


# In[72]:

df['count'].values


# In[73]:

# Let's turn the count column into a float so I can apply some mathematical functions

df['count'] = df['count'].astype('float64') 


# In[74]:

#Now let's check my column datatypes again

df.dtypes


# In[75]:

df['count']


# I'm ready to apply some summary descriptive statistics! Let's take a look...

# In[76]:

df['count'].describe()


# Wow, only 25% of the users have posted 60 times or higher. I want to dig into these quantiles in more detail.

# In[78]:

eighty = df['count'].quantile(q=.8)
ninety = df['count'].quantile(q=.9)
ninetyFive = df['count'].quantile(q=.95)

print '80th: ' + str(eighty)
print '90th: ' + str(ninety)
print '95th: ' + str(ninetyFive)


# So the top 5% of users have posted 1886.2 times or more.
# 
# For fun, let's visualize this distribution to show just how positively skewed it is. I will take two approaches:
# * probability distribution of user posting activity
# * histogram of user post counts

# In[79]:

# Here's the probability distribution

import matplotlib.mlab as mlab

get_ipython().magic(u'pylab inline')
#mu = df.mean()['count']  # mean of distribution
#sigma = df.std()['count']  # standard deviation of distribution
mu = df['count'].mean()
sigma = df['count'].std()
x = mu + sigma * np.random.randn(10000)

num_bins = 50
# the histogram of the data
n, bins, patches = plt.hist(x, num_bins, normed=1, facecolor='green', alpha=0.5)
# add a 'best fit' line
y = mlab.normpdf(bins, mu, sigma)
plt.plot(bins, y, 'r--')
plt.xlabel('count')
plt.ylabel('Probability')
plt.title(r'Histogram of Post Count: $\mu=0.38$, $\sigma=0.48$')

# Tweak spacing to prevent clipping of ylabel
plt.subplots_adjust(left=0.15)
plt.show()


# In[80]:

# Here's a straight-forward histogram plot 
get_ipython().magic(u'pylab inline')

import seaborn as sns

plt.title("Count of Posts per User")
plt.xlabel("# of Posts")
plt.ylabel("# of Users")
plt.hist(df['count'], bins = 10)
#pyplot.legend(loc='upper right')


# # Conclusion
# 
# This data has been reviewed, audited, cleaned, queried and analyzed. I think it's interesting that the top 5% of users account for the bulk of the posts in this dataset. This makes me wonder about the accuracy of this data. Humans are prone to error, so I wonder if the fact that a small concentration of users accounting for the bulk of the posts increases the inaccuracy of the dataset. 
# 
# If I were to conduct this excercise again, I would want to do the following:
# * Approach the SQLite table creation and data insertion in a more programmatic way --> the unicode fields posed a problem for me when trying to implement this programmatically and would love advice on how to approach this in a more automated way
# * Do some more correlation queries around user posting activity and types of amenities
# * Expand my auditing, cleaning and data import process to account for more than just node and way elements
# 

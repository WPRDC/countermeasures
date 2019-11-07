import re, os, sys, json, traceback
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
from pprint import pprint
import time
from datetime import datetime, timedelta
import dataset
from zipfile import PyZipFile

import requests
from lxml import html, etree # Use etree.tostring(element) to dump 
# the raw XML.

from notify import send_to_slack

from parameters.local_parameters import ELECTION_RESULTS_SETTINGS_FILE

from ckanapi import RemoteCKAN

# Change path to script's path for cron job 
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
#############

# BEGIN functions stolen from utility_belt #

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

    return desired_string

def find_resource_id(site,package_id,resource_name,API_key=None):
    resources = get_package_parameter(site,package_id,'resources',API_key)
    #ckan = RemoteCKAN(site, apikey=API_key)
    #metadata = ckan.action.package_show(id=package_id)
    #resources = metadata['resources']
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

# END functions stolen from utility_belt #

class ElectionResultsSchema(pl.BaseSchema): 
    line_number = fields.Integer(dump_to="line_number", allow_none=False)
    contest_name = fields.String(allow_none=False)
    choice_name = fields.String(allow_none=False)
    party_name = fields.String(allow_none=True)
    total_votes = fields.Integer(allow_none=False)
    percent_of_votes = fields.Float(allow_none=True)
    registered_voters = fields.Integer(allow_none=True)
    ballots_cast = fields.Integer(allow_none=True)
    num_precinct_total = fields.Integer(dump_to="total_number_of_precincts", allow_none=True)
    num_precinct_rptg = fields.Integer(dump_to="number_of_precincts_reporting", allow_none=True)
    over_votes = fields.Integer(allow_none=True)
    under_votes = fields.Integer(allow_none=True)
    # NEVER let any of the key fields have None values. It's just asking for 
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no 
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.

    class Meta:
        ordered = True

    # From the Marshmallow documentation:
    #   Warning: The invocation order of decorated methods of the same 
    #   type is not guaranteed. If you need to guarantee order of different 
    #   processing steps, you should put them in the same processing method.
    #@pre_load
    #def plaintiffs_only_and_avoid_null_keys(self, data):
    #    if data['plaintiff'] is None: 
    #        data['plaintiff'] = ''
    #        print("Missing plaintiff")

# FOR SOME PART OF THE BELOW PIPELINE, I THINK...
#The package ID is obtained not from this file but from
#the referenced settings.json file when the corresponding
#flag below is True.
def notify_admins(msg):
    print(msg)
    send_to_slack(msg)

def delete_temporary_file(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

def classify_election(dt):
    year = dt.year
    if datetime(year,2,14) < dt < datetime(year,4,15):
        which = "Primary"
    elif datetime(year,10,19) < dt < datetime(year,12,19):
        which = "General"
    else:
        # If dt is the last modification date of the summary file,
        # this code is currently not working since the last modification
        # to the 2017 Primary Election results CSV file 
        # was 2017-07-27 14:46:00. Thus, the last modification timestamp
        # may not be adequate for inferring the type of election.

        # Presumably a small number of records could be indicative of a 
        # special election.

        which = "Special"
        #notify_admins("Special election detected")
    return which

def build_resource_name(today,last_modified,election_type=None):
    # Some election dates: May 17, 2017 (Primary)
    # November 8, 2016 (General)

    # We can update at a regular interval, but keep in mind that we 
    # should stop updating the data after the election has been 
    # certified but at least a month before the next election. The 
    # file name in the system does not change with the new data and 
    # we should avoid updating the old election file with data from 
    # the new election.

    # Some previous updates from the Scytl site:
    # October 21, 2016 at 10:10 AM No Results Posted. Results will be posted after 8pm on Election Day.
    # November 10, 2016 at 03:49 PM The unofficial election night final results have been posted, which includes Absentee ballots.
    # December 12, 2016 at 02:18 PM The results of this election have been certified by the Board of Elections and are posted.
    
    # Activity detected outside of the range mid-October to mid-December
    # or mid-February to mid-April might therefore be considered a special
    # election.

    # The County is ready to go with the new file and reporting structure 
    # about two weeks prior to the new election. We can get things set up 
    # 2 weeks in advance of each new election in order to be ready when 
    # the data hits.
     
    # The county updates the data every 10-15 minutes on election night, 
    # then less frequently until final certification a month or so 
    # following the election.
    if last_modified is not None:
        date_to_use = last_modified
    else:
        print("build_resource_name: Falling back from last_modified to today's date.")
        raise ValueError("build_resource_name: Falling back from last_modified to today's date, but maybe this is not such a hot idea...")
        date_to_use = today
    year = date_to_use.year
    if election_type is None:
        which = classify_election(date_to_use)
    else:
        which = str(election_type)

    return "{} {} Election Results".format(year, which)

def compute_hash(target_file):
    import hashlib
    BLOCKSIZE = 65536
    hasher = hashlib.md5()
    with open(target_file, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def retrieve_last_hash(table):
    last = table.find_one(hash_name='Election Results CSV zipped')
    return last
 
def retrieve_hash_by_name(table,r_name):
    last = table.find_one(hash_name='Election Results CSV zipped',inferred_results=r_name)
    return last

def save_new_hash(db,table,new_value,r_name,file_mod_date,drop=False):
    if drop:
        table.drop()
    table = db['election']
    table.insert(dict(hash_name='Election Results CSV zipped', value=new_value, save_date=datetime.now().strftime("%Y-%m-%d %H:%M"), last_modified = file_mod_date.strftime("%Y-%m-%d %H:%M"), inferred_results = r_name))
    return table

def is_changed(table,zip_file,r_name):
    # First just try checking the modification date of the file.
    hash_value = compute_hash(zip_file)
    last_hash_entry = retrieve_hash_by_name(table,r_name)

    if last_hash_entry is not None:
        print("last hash = {}".format(last_hash_entry['value']))
    else: 
        notify_admins("A new election has been detected: {}".format(r_name))

    print("new hash  = {}".format(hash_value))

    zf = PyZipFile(zip_file)
    last_mod = None
    last_mod = datetime(*zf.getinfo("summary.csv").date_time)
    if last_hash_entry is None: # No previous hash found
        return True, last_hash_entry, last_mod

    # Check database of hashes.
    if hash_value == last_hash_entry['value']:
        return False, last_hash_entry, last_mod

    try:
        last_mod = datetime(*zf.getinfo("summary.csv").date_time)
        prev_mod = last_hash_entry['last_modified'] 
        if prev_mod is not None and prev_mod != '': 
            previous_modification_date = datetime.strptime(prev_mod, "%Y-%m-%d %H:%M")
            if last_mod <= previous_modification_date:
                return False, last_hash_entry, last_mod

    except:
        print("Unable to compare the last hash entry's file modification date with the current file's last modification date.")
    return True, last_hash_entry, last_mod

def update_hash(db,table,zip_file,r_name,file_mod_date):
    hash_value = compute_hash(zip_file)
    table = save_new_hash(db,table,hash_value,r_name,file_mod_date)
    return

def fetch_download_entities(driver, download_class):
    try:
        #myElem = WebDriverWait(browser, delay).until(EC.presence_of_element_located((By.ID, 'IdOfMyElement')))
        #summary_file_url = driver.find_elements_by_class_name("list-download-link")[0].get_attribute("href") # This used
        # to work, but Scytl changed the class name. Ah, the perils of screen scraping!
        download_entities = driver.find_elements_by_class_name(download_class)
        print("The page loaded successfully.")
    except TimeoutException:
        print("Loading the page took too long!")
        driver.quit()
    return download_entities

def main(schema, **kwparams):
    # Scrape location of zip file (and designation of the election):
    r = requests.get("http://www.alleghenycounty.us/elections/election-results.aspx")
    tree = html.fromstring(r.content)
    #title_kodos = tree.xpath('//div[@class="custom-form-table"]/table/tbody/tr[1]/td[2]/a/@title')[0] # Xpath to find the title for the link
    # As the title is human-generated, it can differ from the actual text shown on the web page.
    # In one instance, the title was '2019 Primary', while the link text was '2019 General'.
    election_index = 1 # Manually increment this to re-pull older elections
    title_kodos = tree.xpath('//div[@class="custom-form-table"]/table/tbody/tr[{}]/td[2]/a/text()'.format(election_index))[0] # Xpath to find the text for the link
    ## to the MOST RECENT election (e.g., "2017 General Election").

    url = tree.xpath('//div[@class="custom-form-table"]/table/tbody/tr[{}]/td[2]/a'.format(election_index))[0].attrib['href']
    # But this looks like this:
    #   'http://results.enr.clarityelections.com/PA/Allegheny/71801/Web02/#/'
    # so it still doesn't get us that other 6-digit number needed for the
    # full path, leaving us to scrape that too, and it turns out that 
    # such scraping is necessary since the directory where the zipped CSV
    # files are found changes too.

    path = dname+"/tmp"
    # If this path doesn't exist, create it.
    if not os.path.exists(path):
        os.makedirs(path)

    # Worse than that, the page is server-side generated, so one must
    # use something like Selenium to find out what the download link is.
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
    chrome_options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': path}
    chrome_options.add_experimental_option('prefs', prefs)
    chromedriver_path = "/usr/local/bin/chromedriver"
    try:
        driver = webdriver.Chrome(chromedriver_path, chrome_options=chrome_options)
    except:
        driver = webdriver.Chrome("/Users/drw/Apps/Internet/chromedriver", chrome_options=chrome_options)

    driver.get(url)
    # At this point, it's not possible to get the link since
    # the page is generated and loaded too slowly.
    # "the webdriver will wait for a page to load by default. It does 
    # not wait for loading inside frames or for ajax requests. It means 
    # when you use .get('url'), your browser will wait until the page 
    # is completely loaded and then go to the next command in the code. 
    # But when you are posting an ajax request, webdriver does not wait 
    # and it's your responsibility to wait an appropriate amount of time 
    # for the page or a part of page to load; so there is a module named 
    # expected_conditions."
    delay = 15 # seconds
    time.sleep(delay)

    download_class = "pl-2"
    download_entities = fetch_download_entities(driver, download_class)
    if len(download_entities) == 0:
        # Fall back to older download_class (2019 Primary election and earlier
        # [yes, the HTML can change from election to election]).
        download_class = "list-download-link"
        download_entities = fetch_download_entities(driver, download_class)

    if len(download_entities) == 0:
        send_to_slack("countermeasures can no longer find the part of the DOM that contains the download links.",username='countermeasures',channel='@david',icon=':satellite_antenna:')
        driver.quit()
        raise RuntimeError("Screen-scraping error. Nothing found in class {}.".format(download_class))

    summary_file_url = download_entities[0].get_attribute("href")

    # Download ZIP file
    #r = requests.get("http://results.enr.clarityelections.com/PA/Allegheny/63905/188108/reports/summary.zip") # 2016 General Election file URL
    #election_type = "Primary"
    #r = requests.get("http://results.enr.clarityelections.com/PA/Allegheny/68994/188052/reports/summary.zip") # 2017 Primary Election file URL

    election_type = "General"
    #path_for_current_results = "http://results.enr.clarityelections.com/PA/Allegheny/71801/189912/reports/"
    #summary_file_url = path_for_current_results + "summary.zip"
    r = requests.get(summary_file_url) # 2017 General Election file URL
    # For now, this is hard-coded.
    #xml_file_url = path_for_current_results + "detailxml.zip"
    xml_index = 2 # Previously this was 3
    #xml_file_url = driver.find_elements_by_class_name(download_class)[xml_index].get_attribute("href")
    xml_file_url = download_entities[xml_index].get_attribute("href")
    found = True
    if re.search("xml",xml_file_url) is None:
        xml_index = 1
        found = False
        #list_download_links = driver.find_elements_by_class_name(download_class)
        while xml_index < len(download_entities) and not found:
            xml_file_url = download_entities[xml_index].get_attribute("href")
            found = re.search("xml",xml_file_url) is not None
            xml_index += 1

    driver.quit()

    print("xml_file_url = {}".format(xml_file_url))
    if not found:
        notify_admins("Scraping Failure: Unable to find an XML file. Countermeasures terminated.")
        raise ValueError("This ETL job is broken on account of scraping failure.")

    # Save result from requests to zip_file location.
    zip_file = dname+'/tmp/summary.zip'
    with open(format(zip_file), 'wb') as f:
        f.write(r.content)

    print("zip_file = {}".format(zip_file))
    today = datetime.now()

    # Make name of hash database dependent on the server
    # as a very clear way of differentiating test and production
    # datasets.
    server = kwparams.get('server', "secret-cool-data")
    db = dataset.connect('sqlite:///{}/hashes-{}.db'.format(dname,server))
    table = db['election']

    # with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(ELECTION_RESULTS_SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']


    changed, last_hash_entry, last_modified = is_changed(table,zip_file,title_kodos)
    if not changed:
        print("The Election Results summary file for {} seems to be unchanged.".format(title_kodos))
        return
    else:
        print("The Election Results summary file for {} does not match a previous file.".format(title_kodos))
        election_type = None # Change this to force a particular election_type to be used, but it's
        # basically irrelevant since r_name_kang is not being used.
        r_name_kang = build_resource_name(today,last_modified,election_type)
        #r_name_kodos = re.sub(" Results"," Election Results",title_kodos)
        # Sample names from titles of links:
        # Special Election for 35th Legislative District
        # 2017 General Results
        # Election Results: 2014 Primary
        # Election Results: 2014 General Election
        # 2012 Special 40th State Sen Results
        
        # Since there's so much variation in these names, maybe it's best just
        # to use them without modifying them and accept that the resource 
        # names will vary a little. They can always be cleaned up after the election.
        r_name_kodos = title_kodos

        print("Inferred name = {}, while scraped name = {}".format(r_name_kang,r_name_kodos))
       
        r_chosen_name = r_name_kodos # Using the scraped name seems better.

        if r_name_kang != r_name_kodos:
            resource_id = find_resource_id(site,package_id,r_chosen_name,API_key=API_key)
            if resource_id is None:
                send_to_slack("countermeasures has found two conflicting names for the resource: {} and {}. Neither can be found in the dataset. {} is being used as the default.\nThis is your reminder to move the new resources to the top of the list.".format(r_name_kodos,r_name_kang,r_name_kodos),username='countermeasures',channel='@david',icon=':satellite_antenna:')
                # The first time this notification fired, the Kodos name was "Special Election for 35th Legislative District" and the Kang name was "2018 General Election Results".
                # The second name was (incorrectly) used for storing the CSV file, while the first name was used for storing the zipped XML file.

    # Unzip the file
    filename = "summary.csv"
    zf = PyZipFile(zip_file).extract(filename,path=path)
    target = "{}/{}".format(path,filename)
    print("target = {}".format(target))
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': r_chosen_name}
    #else:
        #kwargs = {'resource_id': ''}

    # Code below stolen from prime_ckan/*/open_a_channel() but really 
    # from utility_belt/gadgets 


    print("Preparing to pipe data from {} to resource {} (package ID = {}) on {}".format(target,list(kwargs.values())[0],package_id,site))
    time.sleep(1.0)

    pipeline = pl.Pipeline('election_results_pipeline',
                              'Pipeline for the County Election Results',
                              log_status=False,
                              settings_file=ELECTION_RESULTS_SETTINGS_FILE,
                              settings_from_file=True,
                              start_from_chunk=0
                              ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=['line_number'],
              method='upsert',
              **kwargs).run()

    
    update_hash(db,table,zip_file,r_chosen_name,last_modified)

    # Also update the zipped XML file.

    r_xml = requests.get(xml_file_url)
    xml_file = dname+'/tmp/detailxml.zip'
    with open(format(xml_file), 'wb') as g:
        g.write(r_xml.content)

    xml_name = r_chosen_name+' by Precinct (zipped XML file)'

    ckan = RemoteCKAN(site, apikey=API_key)
    resource_id = find_resource_id(site,package_id,xml_name,API_key=API_key)
    if resource_id is None:
        ckan.action.resource_create(
            package_id=package_id,
            url='dummy-value',  # ignored but required by CKAN<2.6
            name=xml_name,
            upload=open(xml_file, 'rb'))
    else:
        ckan.action.resource_update(
            package_id=package_id,
            url='dummy-value',  # ignored but required by CKAN<2.6
            id = resource_id,
            upload=open(xml_file, 'rb'))

    log = open(dname+'/uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished upserting {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished upserting {}\n".format(kwargs['resource_id']))
    log.close()

    
    # Delete temp file after extraction.
    delete_temporary_file(zip_file)
    delete_temporary_file(path+'/'+filename)


schema = ElectionResultsSchema
fields0 = schema().serialize_to_ckan_fields()
# Eliminate fields that we don't want to upload.
#fields0.pop(fields0.index({'type': 'text', 'id': 'party_type'}))
#fields0.pop(fields0.index({'type': 'text', 'id': 'party_name'}))
#fields0.append({'id': 'assignee', 'type': 'text'})
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    try:
        if len(sys.argv) > 1:
            server = sys.argv[1]
            # When invoking this function from the command line, the
            # argument 'production' must be given to push data to
            # a public repository. Otherwise, it will default to going
            # to a test directory.
            main(schema,server=server)
            # Note that the hash database is currently unaware of which
            # server a file is saved to, so if it's first saved to
            # the test server and you run the ETL script again for the
            # production server, if the file hasn't changed, the script
            # will not push the data to the production server.
        else:
            main(schema)
    except:
        e = sys.exc_info()[0]
        print("Error: {} : ".format(e))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_msg = ''.join('!! ' + line for line in lines)
        print(traceback_msg)  # Log it or whatever here
        msg = "countermeasures ran into an error: {}.\nHere's the traceback:\n{}".format(e,traceback_msg)
        mute_alerts = False #kwargs.get('mute_alerts',False)
        if not mute_alerts:
            send_to_slack(msg,username='countermeasures',channel='@david',icon=':satellite_antenna:')


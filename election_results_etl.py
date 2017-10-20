import sys, json, datetime
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl
from subprocess import call
import pprint
import time
from datetime import datetime, timedelta
import dataset
from zipfile import PyZipFile

from parameters.local_parameters import ELECTION_RESULTS_SETTINGS_FILE

class ElectionResultsSchema(pl.BaseSchema): 
    line_number = fields.Integer(dump_to="line_number", allow_none=False)
    contest_name = fields.String(allow_none=False)
    choice_name = fields.String(allow_none=False)
    party_name = fields.String(allow_none=True)
    total_votes = fields.Integer(allow_none=False)
    percent_of_votes = fields.Float(allow_none=True)
    registered_voters = fields.Integer(allow_none=True)
    ballots_cast = fields.Integer(allow_none=True)
    num_precinct_total = fields.Integer(dump_to="num_precinct_total", allow_none=True)
    num_precinct_rptg = fields.Integer(dump_to="num_precinct_rptg",allow_none=True)
    over_votes = fields.Integer(allow_none=True)
    under_votes = fields.Integer(allow_none=True)
    # Never let any of the key fields have None values. It's just asking for 
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
def notify_admins():
    pass

def build_resource_name(today):
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
      
    year = today.year
    if datetime(year,2,14) < today < datetime(year,4,15):
        which = "Primary"
    elif datetime(year,10,19) < today < datetime(year,12,19):
        which = "General"
    else:
        which = "Special"
        notify_admins("Special election detected")

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
 
def save_new_hash(table,new_value,r_name):
    table.insert(dict(hash_name='Election Results CSV zipped', value=new_value, date=datetime.now().strftime("%Y-%m-%d"),inferred_results = r_name))
    return table

def is_changed(table,zip_file):
    # First just try checking the modification date of the 
    # file.
    hash_value = compute_hash(zip_file)
    last_hash_entry = retrieve_last_hash(table)
    print(last_hash_entry)
    zf = PyZipFile(zip_file)
    #last_mod = datetime(*zf.getinfo("summary.csv").date_time)
    #if last_hash_entry is None:
    #    return True, None
    #if last_mod <= datetime.strptime(last_hash_entry['date'], "%Y-%m-%d"): # This compares two different values.
    #    return False, last_hash_entry
    try:
        last_mod = datetime(*zf.getinfo("summary.csv").date_time)
        if last_hash_entry is None:
            return True, None
        if last_mod <= last_hash_entry['date']: # This compares two different values.
            return False, last_hash_entry
    except:
        # Check database of hashes.
        if hash_value == last_hash_entry.value:
            return False, last_hash_entry
    return True, last_hash_entry

def update_hash(table,zip_file,r_name):
    hash_value = compute_hash(zip_file)
    table = save_new_hash(table,hash_value,r_name)
    return

def main(schema):
    zip_file = 'summary.zip'
    print("zip_file = {}".format(zip_file))
    today = datetime.now()

    db = dataset.connect('sqlite:///hashes.db')
    table = db['election']

    changed, last_hash_entry = is_changed(table,zip_file)
    if not changed:
        print("The Election Results summary file seems to be unchanged.")
        return
    else:
        print("A change in the Election Results summary file was detected.")
        r_name = build_resource_name(today)
        print("Inferred name = {}".format(r_name))

    # Unzip the file
    path = "tmp"
    # [ ] If this path doesn't exist, create it.
    filename = "summary.csv"
    zf = PyZipFile(zip_file).extract(filename,path=path)
    target = "{}/{}".format(path,filename)
    print("target = {}".format(target))
    specify_resource_by_name = True
    if specify_resource_by_name:
        kwargs = {'resource_name': r_name}
    #else:
        #kwargs = {'resource_id': ''}
    #resource_id = '8cd32648-757c-4637-9076-85e144997ca8' # Raw liens
    #target = '/Users/daw165/data/TaxLiens/July31_2013/raw-liens.csv' # This path is hard-coded.

    # Call function that converts fixed-width file into a CSV file. The function 
    # returns the target file path.

#    target = '/Users/drw/WPRDC/Tax_Liens/foreclosure_data/raw-seminull-test.csv'


    server = "production"
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(ELECTION_RESULTS_SETTINGS_FILE) as f: 
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,list(kwargs.values())[0],package_id,site))
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

    update_hash(table,zip_file,r_name)
    log = open('uploaded.log', 'w+')
    if specify_resource_by_name:
        print("Piped data to {}".format(kwargs['resource_name']))
        log.write("Finished upserting {}\n".format(kwargs['resource_name']))
    else:
        print("Piped data to {}".format(kwargs['resource_id']))
        log.write("Finished upserting {}\n".format(kwargs['resource_id']))
    log.close()
    # [ ] Delete temp file after extraction.

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
    main(schema)

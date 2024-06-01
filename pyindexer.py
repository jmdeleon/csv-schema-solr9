#!/usr/bin/python3

import sys, getopt
import csv, json
import pysolr

def main(args):
  inputfile='./23100274.csv'
  solr_url='http://my-site.ddev.site:8983/solr/23100274-en'
  field_delimeter = ','
  is_verbose = False
  clear_index = False

  try:
    opts, args = getopt.getopt(args,"hvci:u:d:")
  except getopt.GetoptError:
    print('index_data.py -v -c -i inputfile -u solr_url -d delimeter')
    sys.exit(2)

  for opt, arg in opts:
    if opt in ('-h'):
      print('index_data.py -v -c -i inputfile -u solr_url -d delimeter')
      sys.exit()

    if opt in ("-i"):
      inputfile = arg

    if opt in ("-u"):
      solr_url = arg

    if opt in ("-d"):
      field_delimeter = arg

    if opt in ("-c"):
      clear_index = True

    if opt in ("-v"):
      is_verbose = True
      print('Progress count on...')
      if (clear_index): print('Clear index')
      print('Solr URL: ' + solr_url)
      print('Source file: ' + inputfile)
      print('Delimiter: ' + field_delimeter)

  # create connection to Solr server
  s = pysolr.Solr(solr_url, timeout=10)
  if (clear_index):
    s.delete(q='*:*') # clear index

  # create records to submit to Solr
  record_count=0
  with open(inputfile, encoding='utf-8-sig') as f:

    reader = csv.DictReader(f, quoting=csv.QUOTE_ALL, delimiter=field_delimeter)

    if (is_verbose):
      print(reader.fieldnames)

    for splits in reader:
      record_count += 1
      if (((record_count % 100) == 0) & is_verbose):
        print(record_count,end=' ',flush=True)

      # add record as JSON to Solr index
      s.add({
        "REF_DATE":splits["REF_DATE"],
        "REF_DATESTAMP":splits["REF_DATE"]+"T12:00:00Z",
        "GEO":splits["GEO"],
        "DGUID":splits["DGUID"],
        "companies":splits["Companies"],
        "indicator":splits["Indicator"],
        "UOM":splits["UOM"],
        "UOM_ID":splits["UOM_ID"],
        "SCALAR_FACTOR":splits["SCALAR_FACTOR"],
        "SCALAR_ID":splits["SCALAR_ID"],
        "VECTOR":splits["VECTOR"],
        "COORDINATE":splits["COORDINATE"],
        "VALUE":splits["VALUE"],
        "STATUS":splits["STATUS"],
        "SYMBOL":splits["SYMBOL"],
        "TERMINATED":splits["TERMINATED"],
        "DECIMALS":splits["DECIMALS"],
        "id":splits["VECTOR"]+'-'+splits["COORDINATE"]+'-'+splits["REF_DATE"]
        })

  s.commit()
  if (is_verbose):
    print('Done !!')

if __name__ == "__main__":
  main(sys.argv[1:])

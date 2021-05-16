#!/usr/bin/env python

"""
create a dataset and table and change the respective feilds in  WriteToBigQuery
change project variable and bucket variable
change csv file url, add bucket url of your csv data
"""

import apache_beam as beam


PROJECT='qwiklabs-gcp-04-8f60861e7d5a'
BUCKET='qwiklabs-gcp-04-8f60861e7d5a'
SCHEMA = 'id:STRING,name:STRING,salary:STRING'

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--region=us-central1',
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)


   # find all lines that contain the searchTerm
   (p
      | 'GetData' >> beam.io.ReadFromText('gs://qwiklabs-gcp-04-8f60861e7d5a/for-Public-access/details.csv', skip_header_lines =1)
      |'SplitData' >> beam.Map(lambda x: x.split(','))
      | 'FormatToDict' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "salary": x[2]})
      |'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:Dataset.Table_12'.format(PROJECT),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

   p.run()

if __name__ == '__main__':
   run()

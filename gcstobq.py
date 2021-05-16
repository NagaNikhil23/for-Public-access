  
#!/usr/bin/env python

"""
Copyright Google Inc. 2016
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import apache_beam as beam


PROJECT='cloud-training-demos'
BUCKET='cloud-training-demos'
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
      | 'GetData' >> beam.io.ReadFromText('gs://ag-pipeline/batch/beers.csv', skip_header_lines =1)
      |'SplitData' >> beam.Map(lambda x: x.split(','))
      | 'FormatToDict' >> beam.Map(lambda x: {"id": x[0], "name": x[1], "salary": x[2]})
      |'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:beer.beer_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

   p.run()

if __name__ == '__main__':
   run()

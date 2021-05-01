import logging, re
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class FormatDOB(beam.DoFn):
  def process(self, element):
    sid = element['sid']
    fname = element['fname']
    lname = element['lname']
    dob = element['dob']
    status = element['status']

    # handle YYYY-MM-DD and Month-DD-YYYY (e.g. May-10-1999) 
    split_dob = dob.split('-')
    if len(split_dob) == 3:
        if bool(re.search('\d', split_dob[0])):
            year = split_dob[0]
            month = split_dob[1]
            day = split_dob[2]
        else:
            month = split_dob[0]
            day = split_dob[1]
            year = split_dob[2]
            
            if month in 'Jan':
                month = '01'
            elif month in 'Feb':
                month = '02'
            elif month in 'Mar':
                month = '03'
            elif month in 'Apr':
                month = '04'
            elif month in 'May':
                month = '05'
            elif month in 'Jun':
                month = '06'
            elif month in 'Jul':
                month = '07'
            elif month in 'Aug':
                month = '08'
            elif month in 'Sep':
                month = '09'
            elif month in 'Oct':
                month = '10'
            elif month in 'Nov':
                month = '11'
            elif month in 'Dec':
                month = '12'
    
    # handle MM/DD/YYYY 
    else:
        split_dob = dob.split('/')
        month = split_dob[0]
        day = split_dob[1]
        year = split_dob[2]
    
    dob = year + '-' + month + '-' + day
    print('input dob: ' + element['dob'] + ', output dob: ' + dob)
    record = {'sid': sid, 'fname': fname, 'lname': lname, 'dob': dob, 'status': status}
    return [record]
           
def run():
     PROJECT_ID = 'cs327e-sp2021'
     BUCKET = 'gs://cs327e-sp2021-dataflow/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT sid, fname, lname, dob, status FROM college_beam.Student limit 50'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     out_pcoll = query_results | 'Format DOB' >> beam.ParDo(FormatDOB())

     out_pcoll | 'Log output' >> WriteToText('output.txt')

     dataset_id = 'college_beam'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Student_Beam'
     schema_id = 'sid:STRING,fname:STRING,lname:STRING,dob:DATE,status:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

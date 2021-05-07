import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class MakeTakes(beam.DoFn):
  def process(self, element):
    sid = element['sid']
    cno = element['cno']
    cname = element['cname']
    credits = int(element['credits'])
    grade = element['grade']

    if sid != None and grade != None:
        record = {'sid': sid, 'cno': cno, 'grade': grade}
        return [record]
    if sid != None:
        record = {'sid': sid, 'cno': cno}
        return [record]

class MakeClass(beam.DoFn):
  def process(self, element):
    sid = element.get('sid', '')
    cno = element['cno']
    cname = element['cname']
    credits = int(element['credits'])
    grade = element['grade']

    record = {'cno': cno, 'cname': cname, 'credits': credits}
    return [(cno, record)]

class MakeUniqueClass(beam.DoFn):
  def process(self, element):
     
     cno, classes = element # classes = _UnwindowedValues object
     class_list = list(classes) 
  
     return [class_list[0]]
           
def run():
    PROJECT_ID = 'cs327e-sp2021' # change to your project
    BUCKET = 'gs://cs327e-sp2021-dataflow' # change to your bucket
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='class',
    temp_location=BUCKET + '/temp',
    region='us-central1')

    p = beam.pipeline.Pipeline(options=options)

    sql = 'SELECT sid, cno, cname, credits, grade FROM college_beam.Class'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    takes_pcoll = query_results | 'Make Takes' >> beam.ParDo(MakeTakes())

    takes_pcoll | 'Log takes output' >> WriteToText(DIR_PATH + 'takes_output.txt')

    dataset_id = 'college_beam'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Takes_Dataflow'
    schema_id = 'sid:STRING,cno:STRING,grade:STRING'

    takes_pcoll | 'Write takes to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
    
    class_pcoll = query_results | 'Make Class' >> beam.ParDo(MakeClass())
    
    grouped_class_pcoll = class_pcoll | 'GroupByKey' >> beam.GroupByKey()
    
    grouped_class_pcoll | 'Log class groups' >> WriteToText(DIR_PATH + 'class_groups_output.txt')
    
    unique_class_pcoll = grouped_class_pcoll | 'Make Unique Class' >> beam.ParDo(MakeUniqueClass())
    
    unique_class_pcoll | 'Log class unique' >> WriteToText(DIR_PATH + 'class_unique_output.txt')

    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'Class_Dataflow'
    schema_id = 'cno:STRING,cname:STRING,credits:INTEGER'

    unique_class_pcoll | 'Write class to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
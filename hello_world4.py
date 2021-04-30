import apache_beam as beam
from apache_beam.io import WriteToText
import logging

class SplitWords(beam.DoFn):
  def process(self, element):
     
     results = []
     words = element.split()
     
     for word in words:
        results.append((word, 1))
     
     return results

class SumWords(beam.DoFn):
  def process(self, element):
     
     word, ones = element
     return [(word, sum(ones))]
           
def run():
     PROJECT_ID = 'cs327e-sp2021' # change to your project id

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     in_pcoll = p | beam.Create(['hello world', 'hello beam users', 'beam is fun'])

     split_pcoll = in_pcoll | 'Split Words' >> beam.ParDo(SplitWords())
     
     grouped_pcoll = split_pcoll | 'Group Words' >> beam.GroupByKey()
        
     out_pcoll = grouped_pcoll | 'Sum Words' >> beam.ParDo(SumWords())
    
     out_pcoll | 'Write results' >> WriteToText('sum.txt')
    
     result = p.run()
     result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

import apache_beam as beam
from apache_beam.io import WriteToText
import logging

class SplitWords(beam.DoFn):
  def process(self, element):
     words = element.split()
     return [words]
           
def run():
     PROJECT_ID = 'cs327e-sp2021' # change to your project id

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     in_pcoll = p | beam.Create(['hello world', 'hello beam users', 'beam is fun'])

     out_pcoll = in_pcoll | 'Split Words' >> beam.ParDo(SplitWords())
        
     out_pcoll | 'Write results' >> WriteToText('words.txt')
    
     result = p.run()
     result.wait_until_finish() 

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()

    
 

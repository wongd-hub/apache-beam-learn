import argparse

import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def main(argv=None, save_main_session=True):
    
    # Define new arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='input/agatha_complete.txt',
        help='The file path for the input text to process'
    )
    parser.add_argument(
        '--output',
        dest='output'
    )

    # Split arguments into the args defined above, and all other args 
    # (which we assume go directly into `PipelineOptions`)
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Run pipeline args through PipelineOptions
    pipeline_options = PipelineOptions(pipeline_args)

    # Use save_main_session if our workflow relies on the global context 
    # (e.g. a module defined at the global level)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        lines = (
            p
            # Using an additional arg directly in pipeline
            | 'Read' >> ReadFromText(known_args.input)
            | beam.Filter(lambda line: line != "")
        )

        output = lines | 'Write' >> WriteToText(known_args.output)

        result = p.run()
        result.wait_until_finish()

# Run the file only when running, not when importing
if __name__ == '__main__':
  main()
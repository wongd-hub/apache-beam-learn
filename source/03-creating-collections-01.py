import apache_beam as beam

# Output PCollection - don't worry about this, we'll learn this 
# in future lessons. Just know that this outputs the PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

with beam.Pipeline() as p:

    (
        p
        | 'Create range' >> beam.Create(range(1, 11))
        | 'Output range' >> Output()
    )

    (
        p
        | 'Create words' >> beam.Create(['To', 'be', 'or', 'not', 'to', 'be'])
        | 'Output words' >> Output()
    )
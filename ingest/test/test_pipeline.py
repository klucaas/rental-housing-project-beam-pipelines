from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.io import ReadFromTextWithFilename

from google.cloud import storage
import os
import unittest


class TestPostIngestTransformPipeline(unittest.TestCase):

    def setUp(self):
        self.TEST_BUCKET = 'gs://test-rental-housing-project'
        self.upload_test_files_to_bucket()

    def upload_test_files_to_bucket(self):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.get_bucket(self.TEST_BUCKET)
        files_to_upload = [f for f in os.listdir('test/sample_gcs_records') if f.endswith('.txt')]
        for f in files_to_upload:
            blob = self.bucket.blob(f)
            blob.upload_from_filename(f)



# with TestPipeline() as p:
#     pcoll = p | ReadFromTextWithFilename(file_pattern=TEST_BUCKET)
#     self.assertEqual(pcoll, gcs_pcoll)

if __name__ == '__main__':
    unittest.main()
"""
pipeline.py is an Apache Beam pipeline which is part of the project "rental-housing-project".
The pipeline reads source objects from Google Cloud Storage, applies a series of transformations,
and writes the output PCollections to MongoDB and BiqQuery. The pipeline can be executed with:

python pipeline.py --project=GCP_PROJECT_NAME \
                   --runner=PIPELINE_RUNNER \
                   --input_gcs_bucket=GCS_BUCKET_NAME \
                   --bq_dataset=BIGQUERY_DATASET_NAME \
                   --bq_rentals=BIGQUERY_RENTALS_TABLE_NAME \
                   --bq_enrichment=BIGQUERY_ENRICHMENT_TABLE_NAME \
                   --bq_ngrams=BQ_NGRAMS_TABLE_NAME \
                   --mongo_uri=MONGO_CONNECTION_URI \
                   --mongo_db=MONGO_DB_NAME \
                   --mongo_coll=MONGO_COLLECTION_NAME

*note: mongo_uri format should be 'mongodb+srv://<USERNAME>:<PASSWORD>@<HOST>/<DATABASE>'
*note: when using runners other than DirectRunner, additional arguments can be passed for customization
"""
import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.gcsio import GcsIO
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError
from bs4 import BeautifulSoup
import logging
import json
import time
import re


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
              '--input_gcs_bucket',
              help='Input Google Cloud Storage Bucket to read from')
        parser.add_argument(
              '--bq_dataset',
              help='BigQuery Dataset where tables are located')
        parser.add_argument(
              '--bq_rentals',
              help='BigQuery table where rentals data is stored')
        parser.add_argument(
              '--bq_enrichment',
              help='BigQuery table where enrichment data is stored')
        parser.add_argument(
              '--bq_ngrams',
              help='BigQuery table name where Ngrams are stored')
        parser.add_argument(
              '--mongo_uri',
              help='MongoDB Connection URI')
        parser.add_argument(
              '--mongo_db',
              help='MongoDB database')
        parser.add_argument(
              '--mongo_coll',
              help='MongoDB collection to use')


class BatchDeleteGCSFiles(beam.DoFn):
    """
    Batch delete processed objects from Google Cloud Storage.

    To avoid a REST API call for every object to be deleted, we create a buffer to hold the names of the objects
    to be deleted. When this buffer reaches it's max size, we perform `apache_beam.io.gcp.gcsio.GcsIO.batch_delete` to
    delete all objects in the buffer from Google Cloud Storage.

    Note: the `batch_delete` method set by Google has a maximum  of 100 deletes per API call
    """
    def start_bundle(self):
        self.MAX_BUFFER_SIZE = 100
        self._buffer = []
        self.storage_client = GcsIO()

    def process(self, element: dict, *args, **kwargs) -> None:
        self._buffer.append(element)
        if len(self._buffer) == self.MAX_BUFFER_SIZE:
            self.storage_client.delete_batch(paths=self._buffer)
            self._buffer.clear()

    def finish_bundle(self):
        if len(self._buffer) > 0:
            self.storage_client.delete_batch(paths=self._buffer)
            self._buffer.clear()


class SplitTuple(beam.DoFn):
    """
    Split the element into two:
        1) TaggedOutput of name of file read from Google Cloud Storage
        2) Content of the file
    """
    OUTPUT_TAG_GCS_FILES_READ = 'gcs_files_read'

    def process(self, element: dict, *args, **kwargs):
        yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_GCS_FILES_READ, element[0])
        yield element[1]  # goes to "main output"


class JsonCoder(beam.coders.Coder):
    """
    Coder for reading and writing JSON
    """
    def encode(self, value):
        return json.dumps(value)

    def decode(self, encoded):
        return json.loads(encoded)


class ExtractPrice(beam.DoFn):
    """
    Extract and format the Price attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['price'] = int(BeautifulSoup(element['html'], 'html.parser').
                                   find('span', class_='price').get_text().strip('$'))

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the price attribute is NoneType (Not Found).
        except AttributeError:
            element['price'] = None
        finally:
            yield element


class ExtractBedrooms(beam.DoFn):
    """
    Extract and format the Bedrooms attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['bedrooms'] = int(BeautifulSoup(element['html'], 'html.parser').
                                      find('span', class_='shared-line-bubble').get_text().split('/')[0].strip('BR '))

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the bedrooms attribute is NoneType (Not Found).
        #  IndexError is raised as a result of attempting to access an index out of range from `split()`
        #  ValueError is raised as a result of attempting to cast a string to an int on a non-int
        except (AttributeError, IndexError, ValueError):
            element['bedrooms'] = None
        finally:
            yield element


class ExtractBathrooms(beam.DoFn):
    """
    Extract and format the Bathrooms attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['bathrooms'] = float(BeautifulSoup(element['html'], 'html.parser').
                                         find('span', class_='shared-line-bubble').
                                         get_text().split('/')[1].strip('Ba').strip())

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the bathrooms attribute is NoneType (Not Found).
        #  IndexError is raised as a result of attempting to access an index out of range from `split()`
        #  ValueError is raised as a result of attempting to cast a string to an float on a non-float
        except (AttributeError, IndexError, ValueError):
            element['bathrooms'] = None
        finally:
            yield element


class ExtractSQFT(beam.DoFn):
    """
    Extract and format the SQFT attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['sqft'] = int(BeautifulSoup(element['html'], 'html.parser').
                                  find_all('span', class_='shared-line-bubble')[1].get_text().strip('ft2'))

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the SQFT attribute is NoneType (Not Found).
        #  IndexError is raised as a result of attempting to access an index out of range from `split()`
        #  ValueError is raised as a result of attempting to cast a string to an int on a non-int
        except (AttributeError, IndexError, ValueError):
            element['sqft'] = None
        finally:
            yield element


class ExtractAvailability(beam.DoFn):
    """
    Extract and format the Availability attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['availability'] = str(BeautifulSoup(element['html'], 'html.parser').
                                          find_all('span', class_='shared-line-bubble')[2].
                                          get_text().strip('available').strip())

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the availability attribute is NoneType (Not Found).
        #  IndexError is raised as a result of attempting to access an index out of range from `find_all()`
        except (AttributeError, IndexError):
            element['availability'] = None
        finally:
            yield element


class ExtractFeaturesList(beam.DoFn):
    """
    Extract and format the Features attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['features'] = [x for x
                                   in BeautifulSoup(element['html'], 'html.parser').
                                   find_all('p', class_='attrgroup')[1].get_text().split('\n')
                                   if x.strip()]

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the features attribute is NoneType (Not Found).
        #  IndexError is raised as a result of attempting to access an index out of range from `find_all()`
        except (AttributeError, IndexError):
            element['features'] = []
        finally:
            yield element


class ExtractLatitudeLongitude(beam.DoFn):
    """
    Extract and format the Latitude/Longitude attributes.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['latlong'] = ','.join([BeautifulSoup(element['html'], 'html.parser').
                                          find('div', {'id': 'map'})['data-latitude'],
                                           BeautifulSoup(element['html'], 'html.parser').
                                          find('div', {'id': 'map'})['data-longitude']])

        # TypeError raised when the map element is not present
        except TypeError:
            element['latlong'] = None
        finally:
            yield element


class ExtractDescription(beam.DoFn):
    """
    Extract and format the Description attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['description'] = str(BeautifulSoup(element['html'], 'html.parser').
                                         find('section', {'id': 'postingbody'}).get_text().replace('\n', ' ').strip())

        #  AttributeError is raised as a result of attempting to perform `get_text()` when the HTML Tag containing
        #  the description attribute is NoneType (Not Found).
        except AttributeError:
            element['description'] = None
        finally:
            yield element



class ExtractNgrams(beam.DoFn):
    """
    Read frequent n-grams from text file on Google Cloud Storage. Preprocess description by replacing any non word
    character with a single space and lowering all alpha characters. Append any n-grams found in preprocessed
    description to element['ngrams'].

    The idea is to use these n-grams downstream as a search mechanism. Some examples of frequent n-grams include:
    'corner unit', 'hardwood floors', 'stainless steel appliances', 'air conditioning', 'swimming pool', etc.
    """

    def process(self, element: dict, *args, **kwargs):
        ngrams = args[0]
        element['ngrams'] = []
        if element['description']:
            try:
                processed_description = re.sub(r'\W+', ' ', element['description']).lower()
                for ngram in ngrams:
                    ngram_val = ngram.get('ngram')
                    if ngram_val in processed_description:
                        element['ngrams'].append(ngram_val)
            #  AttributeError is raised as a result of attempting to perform `lower()` on a non-string
            except AttributeError:
                pass
            finally:
                yield element


class ExtractImageURLs(beam.DoFn):
    """
    Extract and format the Images attribute.
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['images'] = list(set(
                [k['src'].replace('50x50c', '600x450') for k
                 in BeautifulSoup(element['html'], 'html.parser').find_all('img')]))

        # KeyError is raised as a result of the images HTML tag not having a `src` attribute
        except KeyError:
            element['images'] = []
        finally:
            yield element


class FormatPostedAtDatetimeString(beam.DoFn):
    """
    Convert datetime string format from `YYYY-mm-dd HH:MM` to `YYYY-mm-ddTHH:MM:SS` to meet BigQuery datetime
    formatting requirements
    """
    def process(self, element: dict, *args, **kwargs):
        try:
            element['posted_at'] = element['posted_at'].replace(' ', 'T') + ':00'

        # TypeError is raised when `element['posted_at']` is None because of the attempt to concatenate a NoneType and
        # a string type
        except TypeError as err:
            # `Posted_at` attribute is very important so if this error is raised it indicates something serious has
            #  happened somewhere upstream which has eventually coerced this attribute to NoneType.
            logging.error(msg=f'FormatPostedAtDatetimeString: error: {err} : data: {element}')
        finally:
            yield element


class Enrichment(beam.DoFn):
    """
    Location Enrichment. Check the cache using the lat/long to pull
    """
    OUTPUT_TAG_ADD_TO_CACHE = 'cache_misses'

    def start_bundle(self):
        self.keep_keys = ['house_number', 'road', 'neighbourhood', 'city', 'county',
                          'state', 'suburb', 'postcode', 'country']
        self._geolocator_client = Nominatim(user_agent='rental-housing-project')
        
    def process(self, element: dict, *args, **kwargs):

        def check_cache(latlong):
            """Scan through the cache attempting to match a lat/long pair"""
            for val in args[0]:
                if val.get('latlong') == latlong:
                    return val
        try:
            if element['latlong']:
                # Check the cache
                cache_hit = check_cache(element['latlong'])
                if cache_hit:
                    # Merge the enrichment data from the cache with the element
                    element = {**cache_hit, **element}
                else:
                    # Cache doesn't contain lat/long pair, wait a short period before making GeoPy API call
                    time.sleep(0.5)
                    geopy_call_api = self._geolocator_client.reverse(element['latlong'], timeout=20)
                    if geopy_call_api.raw['address']:
                        # Extract the items from the API response we are keeping
                        enrichment = {k: geopy_call_api.raw['address'][k]
                                               for k in self.keep_keys if k in geopy_call_api.raw['address']}
                        # Merge the enrichment data from the API response with the element
                        element = {**enrichment, **element}
                        # Update the enrichment dict to reflect the original lat/long pair we looked up
                        enrichment.update({'latlong': element['latlong']})
                        # yield the enrichment dict as a side output which we will insert into cache later on
                        yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_ADD_TO_CACHE, enrichment)

        #  GeocoderServiceError occurs when the service cannot be reached or max requests has been reached
        #  KeyError occurs when the raw response from Geopy doesn't have an `address` attribute
        except (GeocoderServiceError, KeyError) as err:
            logging.error(msg=err)
        finally:
            yield element


class DeleteKeysFromElement(beam.DoFn):
    """
    Delete a set of keys from an element
    """
    def process(self, element: dict, *args, **kwargs):
        for arg in args:
            element.pop(arg, None)

        yield element


def filter_on_missing_required_keys(element):
    """
    Filter out elements missing the required keys
    """
    required_keys = {'href', 'posted_at', 'data_id'}
    return all([element.get(key) for key in required_keys])


def run():
    """Entry point to pipeline"""

    # Read in two BigQuery Schemas which will be passed to WriteToBigQuery for validation
    with open('schemastore/bq_schema_enrichment.json') as fin:
        bq_schema_enrichment = fin.read()

    with open('schemastore/bq_schema_rentals.json') as fin:
        bq_schema_rentals = fin.read()

    with beam.Pipeline(options=UserOptions()) as p:

        opts = p.options.view_as(UserOptions)

        """Start the pipeline - read names and contents of all objects in a google cloud storage bucket 
           with .txt extension as a tuple where:
           
           tuple[0] is the name of the object being read in
           tuple[1] is the contents of the object
           
           The main output `elements` is a PCollection where each element is the contents of an object read from Google
           Cloud Storage.
           
           The secondary output `gcs_files_read` is a PCollection of names of the objects read from Google Cloud Storage
           """
        file_pattern = f'gs://{opts.input_gcs_bucket}/*.txt'
        elements, gcs_files_read = (
                p
                | 'ReadFromGCS' >> beam.io.ReadFromTextWithFilename(file_pattern=file_pattern, coder=JsonCoder())
                | 'SplitTuple' >> beam.ParDo(SplitTuple()).with_outputs(SplitTuple.OUTPUT_TAG_GCS_FILES_READ,
                                                                        main='elements')
        )

        """Deferred Side Input. Read from BigQuery Table containing location enrichment data that was obtained from
           OpenStreetMap during previous pipeline executions"""
        cache = beam.pvalue.AsIter((
                p
                | 'ReadBigQueryEnrichmentTable' >> beam.io.Read(beam.io.BigQuerySource(dataset=opts.bq_dataset,
                                                                                       table=opts.bq_enrichment))
        ))

        """Deferred Side Input. Queried from BigQuery Table containing high frequency occurrences of Ngrams"""
        query = f'SELECT ngram from {p.options.bq_dataset}.{p.options.bq_ngrams} ORDER BY frequency LIMIT 25;'
        ngrams = beam.pvalue.AsIter((
                p
                | 'ReadBigQueryNgramsTable' >> beam.io.Read(beam.io.BigQuerySource(query=query))
        ))

        """Do the major transforms - extract target attributes from HTML, enrich location data using reverse lat/long
           search with OpenStreetMap.
           
           The main output `transformed` is a PCollection where each element from the `elements` PCollection has been 
           applied to a series of transformations.
           
           The secondary output `cache_misses` is a PCollection of new enrichment data obtained from OpenStreetMap.
           """
        transformed, cache_misses = (
                elements
                | 'FilterOnMissingRequiredKeys' >> beam.Filter(filter_on_missing_required_keys)
                | 'ExtractPrice' >> beam.ParDo(ExtractPrice())
                | 'ExtractBedrooms' >> beam.ParDo(ExtractBedrooms())
                | 'ExtractBathrooms' >> beam.ParDo(ExtractBathrooms())
                | 'ExtractSQFT' >> beam.ParDo(ExtractSQFT())
                | 'ExtractAvailability' >> beam.ParDo(ExtractAvailability())
                | 'ExtractFeaturesList' >> beam.ParDo(ExtractFeaturesList())
                | 'ExtractLatitudeLongitude' >> beam.ParDo(ExtractLatitudeLongitude())
                | 'ExtractDescription' >> beam.ParDo(ExtractDescription())
                | 'ExtractImageURLs' >> beam.ParDo(ExtractImageURLs())
                | 'FormatPostedAtDatetimeString' >> beam.ParDo(FormatPostedAtDatetimeString())
                | 'DeleteHtmlKey' >> beam.ParDo(DeleteKeysFromElement(), 'html')
                | 'ExtractNgrams' >> beam.ParDo(ExtractNgrams(), ngrams)
                | 'Enrichment' >> beam.ParDo(Enrichment(), cache).with_outputs(Enrichment.OUTPUT_TAG_ADD_TO_CACHE,
                                                                               main='transformed')
        )

        """Write `cache_misses` PCollection to BigQuery"""
        cache_misses | 'WriteCacheMissesToBigQuery' >> beam.io.WriteToBigQuery(
            table=opts.bq_enrichment,
            dataset=opts.bq_dataset,
            schema=beam.io.gcp.bigquery_tools.parse_table_schema_from_json(bq_schema_enrichment),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            validate=True)

        """Write `transformed` PCollection to MongoDB"""
        transformed | 'WriteToMongo' >> beam.io.WriteToMongoDB(uri=opts.mongo_uri,
                                                               db=opts.mongo_db,
                                                               coll=opts.mongo_coll)

        """Remove items from `transformed` PCollection, Write to BigQuery"""
        _ = (
                transformed
                | 'DeleteKeysFromElement' >> beam.ParDo(DeleteKeysFromElement(), '_id', 'href', 'images', 'ngrams')
                | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                    table=opts.bq_rentals,
                    dataset=opts.bq_dataset,
                    schema=beam.io.gcp.bigquery_tools.parse_table_schema_from_json(bq_schema_rentals),
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    validate=True)
        )

        """Delete files that have been processed from Google Cloud Storage input bucket"""
        _ = (
                gcs_files_read
                | 'DistinctFiles' >> beam.transforms.Distinct()
                | 'BatchDeleteGCSFiles' >> beam.ParDo(BatchDeleteGCSFiles())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

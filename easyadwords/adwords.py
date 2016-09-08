from datetime import datetime
from time import sleep

from io import BytesIO
import gzip
import unicodecsv as csv
import re
from ast import literal_eval
from contextlib import closing
from functools import wraps

from googleads import adwords
from googleads.errors import AdWordsReportError
from urllib2 import URLError

from easyadwords.utils import serialize_soap_resp


def retry(retries=3, delay=3, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            retry_num = 1
            last_error = None
            max_retries = getattr(args[0], '_max_retries', retries)

            while retry_num <= max_retries:
                try:
                    return f(*args, **kwargs)
                except AdWordsReportError as e:
                    if e.code >= 500:
                        sleep_time = delay * retry_num * backoff
                        print 'Error encountered retrieving report, sleeping for %ss. Attempt %d [%s]' % (
                            sleep_time,
                            retry_num,
                            e
                        )
                        retry_num += 1
                        last_error = e
                        sleep(sleep_time)
                    else:
                        raise e
                except URLError as e:
                    sleep_time = delay * retry_num * backoff
                    print 'Error encountered retrieving report, sleeping for %ss. Attempt %d [%s]' % (
                        sleep_time,
                        retry_num,
                        e
                    )
                    retry_num += 1
                    last_error = e
                    sleep(sleep_time)
            else:
                raise last_error

        return f_retry  # true decorator
    return deco_retry


class AdwordsUtility:
    def __init__(self, credential_path, client_customer_id=None, service_version=None, max_retries=3):
        """
        Initialize new utility object for interacting with Adwords.

        Configuration/authorization is determined from googleads.yaml (credential_path).

        :param credential_path: Path to googleads.yaml
        :param client_customer_id: Default customer_id, would override that stated in credential_path.
        :param service_version: If set, get specific version. Else, get the latest available version. **NOTE** Check change logs for APIs and googleads client before upgrading or switching report versions.
        """

        self._client = adwords.AdWordsClient.LoadFromStorage(credential_path)

        if service_version is None:
            self.service_version = sorted(adwords._SERVICE_MAP.keys())[-1]
        else:
            assert service_version in adwords._SERVICE_MAP.keys()
            self.service_version = service_version

        assert self._client.client_customer_id is not None or client_customer_id is not None
        if client_customer_id is not None:
            self._client.SetClientCustomerId(client_customer_id)

        self._PAGE_SIZE = 500

        self._max_retries = max_retries

    @retry()
    def change_client_customer_id(self, client_customer_id):
        """
        Set new client_customer_id.
        """
        self._client.SetClientCustomerId(client_customer_id)

    def _iterate_pages(self, service, selector, serialize=True):
        offset = int(selector['paging']['startIndex'])

        return_list = []

        more_pages = True
        while more_pages:
            page = service.get(selector)

            # Compile results
            if 'entries' in page:
                for entry in page['entries']:

                    if serialize:
                        entry = serialize_soap_resp(entry)

                    return_list.append(entry)

            offset += self._PAGE_SIZE
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])

        return return_list

    @retry()
    def get_report_fields(self, report_type, serialize=True):
        """
        Get details about report fields.

        :param report_type: Reference: https://developers.google.com/adwords/api/docs/appendix/reports#report-types
        :param serialize: Convert to dictionary.
        :return: list of dictionaries or SOAP responses depending on serialize option.
        """

        report_definition_service = self._client.GetService('ReportDefinitionService', version=self.service_version)

        # Get report fields.
        fields = report_definition_service.getReportFields(report_type)

        if serialize:
            fields = map(serialize_soap_resp, fields)

        return fields

    @retry()
    def get_service(self, service_name, selector, iterate_pages=True, serialize=True):
        """
        General purpose function for getting any service listed here: https://developers.google.com/adwords/api/docs/reference/

        :param service_name: Name of service
        :param selector:
        :param iterate_pages:
        :param serialize:
        :return:
        """

        service = self._client.GetService(service_name, version=self.service_version)

        if iterate_pages:
            return self._iterate_pages(service, selector, serialize)

        else:
            results = service.get(selector)

            if serialize:
                if isinstance(results, list):
                    return map(serialize_soap_resp, results)
                else:
                    return serialize_soap_resp(results)
            else:
                return results

    def list_account_labels(self):
        """
        Convenience function for AccountLabelService with predefined options.

        :return: list of dictionaries
        """

        selector = {
            'fields': ['LabelName', 'LabelId'],
            'paging': {
                'startIndex': '0',
                'numberResults': '1000'
            }
        }

        return self.get_service('AccountLabelService', selector, iterate_pages=False).get('labels', [])

    def list_accounts(self, fields=None, predicates=None, include_hidden=False, include_mcc=False, serialize=True):
        """
        Convenience function for ManagedCustomerService with predefined options.

        :param predicates: Predicate objects for filtering data.
        :type predicates: list of dictionaries representing Predicate objects
        :param include_hidden: Include hidden accounts in results.
        :param include_mcc: Include MCC in results.
        :param serialize: Convert to dictionary.
        :return: list of dictionaries or SOAP responses depending on serialize option.
        """

        if predicates is not None:
            assert isinstance(predicates, list)
            assert all(isinstance(x, dict) for x in predicates)

        # Default values
        fields = ['Name', 'CustomerId'] if fields is None else fields
        predicates = [] if predicates is None else predicates

        if not include_hidden:
            predicates.append(
                {
                    'field': 'ExcludeHiddenAccounts',
                    'operator': 'EQUALS',
                    'values': 'TRUE'
                }
            )

        if not include_mcc:
            predicates.append(
                {
                    'field': 'CanManageClients',
                    'operator': 'EQUALS',
                    'values': 'FALSE'
                }
            )

        # Construct selector
        selector = {
            'fields': fields,
            'predicates': predicates,
            'paging': {
                'startIndex': '0',
                'numberResults': str(self._PAGE_SIZE)
            }
        }

        return self.get_service('ManagedCustomerService', selector, serialize)

    @retry()
    def get_report(self, start_date, end_date, report_type, fields, additional_fields=None, predicates=None,
                   client_customer_id=None, include_zero_impressions=False):
        """
        Downloads and cleans report.

        Field Examples:

            Renaming field:

                {'name': 'Ctr', 'alias': 'ctr'}

            Custom Cleaning:

                **NOTE** - simplest implementation would be using a lambda function as shown below.

                {'name': 'Ctr', 'alias': 'ctr', 'cleaning': lambda x: float(str(x).replace('%', '').strip())}


        Additional Field Examples:

            Prepending field "updated_at":

                {'name': 'updated_at', 'value': datetime.now(), 'prepend'=True}

        :param start_date: Reporting start date.
        :type start_date: datetime
        :param end_date: Reporting end date.
        :type end_date: datetime
        :param report_type: Reference: https://developers.google.com/adwords/api/docs/appendix/reports#report-types
        :param fields: Fields within report.
        :type fields: list of dictionaries
        :param additional_fields: New fields to add. **Only supports static values, not functions or references to other columns.**
        :type additional_fields: list of dictionaries
        :param predicates: Predicate objects for filtering data.
        :type predicates: list of dictionaries representing Predicate objects
        :param client_customer_id: Overwrite set client_customer_id when downloading report.
        :param include_zero_impressions: **Check compatibility with report type**
        :return: Generator object for cleaned report
        """

        def _default_cleaner(field_value, field_type):
            field_value = field_value.strip()

            if field_value == '--':
                return None

            elif 'List' in field_type:
                if field_value is None or field_value == '':
                    return None
                else:
                    return ';'.join(literal_eval(field_value))

            elif field_type == 'Money':
                # Money is returned as micro units
                # divide and round to 6 dp to avoid representation errors when dividing
                return round(float(re.sub(r'[^\d\-.]+', '', field_value)) / 1000000.0, 6)

            elif field_type == 'Date':
                return datetime.strptime(field_value, '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')

            elif field_type == 'Double':
                return float(re.sub(r'[^\d\-.]+', '', field_value))

            elif field_type in ('Long', 'Integer'):
                return int(float(re.sub(r'[^\d\-.]+', '', field_value)))

            else:
                return field_value

        assert isinstance(start_date, datetime)
        assert isinstance(end_date, datetime)

        if client_customer_id is None:
            client_customer_id = self._client.client_customer_id

        if predicates is not None:
            assert isinstance(predicates, list)
            assert all(isinstance(x, dict) for x in predicates)

        # checks additional fields
        if additional_fields is None:
            additional_fields = []
        else:
            assert isinstance(additional_fields, list)
            assert all(isinstance(x, dict) for x in additional_fields)
            assert all('name' in x and 'value' in x for x in additional_fields)

        report_downloader = self._client.GetReportDownloader(version=self.service_version)

        report = {
            'reportName': '%s %s-%s' % (report_type, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),
            'dateRangeType': 'CUSTOM_DATE',
            'reportType': report_type,
            'downloadFormat': 'GZIPPED_CSV',
            'selector': {
                'fields': map(lambda x: x['name'], fields),
                'dateRange': {
                    'min': start_date.strftime('%Y%m%d'),
                    'max': end_date.strftime('%Y%m%d')
                },
                'predicates': [] if predicates is None else predicates
            }
        }

        # stream compressed report to buffer, seek(0), decompress and load it into csv.reader
        report_data = BytesIO()

        with closing(report_downloader.DownloadReportAsStream(
            report,
            skip_column_header=True,
            skip_report_header=True,
            skip_report_summary=True,
            client_customer_id=client_customer_id,
            include_zero_impressions=include_zero_impressions
        )) as stream_data:

            while True:
                chunk = stream_data.read(1024 * 16)
                if not chunk:
                    break
                report_data.write(chunk)

        report_data.seek(0)
        csv_reader = csv.reader(gzip.GzipFile(fileobj=report_data, mode='rb'))

        # clean data
        report_fields = self.get_report_fields(report_type)
        report_dtypes = {x['fieldName']: x['fieldType'] for x in report_fields}

        # ensure all fields are actually found in report
        assert all(x['name'] in report_dtypes.keys() for x in fields)

        # fill in adwords type if not explicitly stated
        for query_field in fields:
            if 'type' not in query_field:
                query_field['type'] = report_dtypes[query_field['name']]

        # yield header first. alias if exists, else name
        header = map(lambda x: x['alias'] if 'alias' in x else x['name'], fields)

        # add additional field headers
        for additional_field in additional_fields:
            if additional_field.get('prepend', None) is True:
                header.insert(0, additional_field['name'])
            else:
                header.append(additional_field['name'])

        yield header

        for row in csv_reader:
            cleaned_row = []
            for index, field_config in enumerate(fields):
                if 'cleaning' in field_config:
                    cleaned_value = field_config['cleaning'](row[index])
                else:
                    cleaned_value = _default_cleaner(row[index], field_config['type'])

                cleaned_row.append(cleaned_value)

            # add additional field values
            for additional_field in additional_fields:
                if additional_field.get('prepend', None) is True:
                    cleaned_row.insert(0, additional_field['value'])
                else:
                    cleaned_row.append(additional_field['value'])

            yield cleaned_row

    def get_all_account_info(self, start_date, end_date):
        """
        Convenience function wrapping ACCOUNT_PERFORMANCE_REPORT to get and parse accounts info.
        Can be used to subsequently filter out accounts without any activity for specific days.

        :param start_date: Start date
        :type start_date: datetime object
        :param end_date: End date
        :type start_date: datetime object
        :return: Dictionary structured by account id > date > metrics
        """

        fields = [
            {
                "name": "Date",
                "alias": "date"
            },
            {
                "name": "ExternalCustomerId",
                "alias": "account_id"
            },
            {
                "name": "Cost",
                "alias": "cost"
            },
            {
                "name": "Impressions",
                "alias": "impressions"
            },
            {
                "name": "Clicks",
                "alias": "clicks"
            },
            {
                "name": "Conversions",
                "alias": "conversions"
            }
        ]

        account_lookup = {}

        account_list = self.list_accounts()

        for account in account_list:
            report = self.get_report(
                start_date,
                end_date,
                'ACCOUNT_PERFORMANCE_REPORT',
                fields,
                client_customer_id=account['customerId'],
                include_zero_impressions=True
            )

            header = next(report)
            for row in report:
                row_dict = dict(zip(header, row))
                report_account_id = row_dict.pop('account_id')
                report_date = datetime.strptime(row_dict.pop('date'), '%Y-%m-%d %H:%M:%S')

                account_lookup.setdefault(report_account_id, {})
                account_lookup[report_account_id][report_date] = row_dict

        return account_lookup

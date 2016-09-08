from suds.sudsobject import asdict
from datetime import datetime, timedelta


def serialize_soap_resp(resp):
    """
    Convert Adwords SOAP response to serializable dict

    :param resp: SOAP response
    :return: Dictionary representation of response
    """
    out = {}
    for k, v in asdict(resp).iteritems():
        if hasattr(v, '__keylist__'):
            out[k] = serialize_soap_resp(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, '__keylist__'):
                    out[k].append(serialize_soap_resp(item))
                else:
                    out[k].append(item)
        else:
            try:
                out[k] = v.encode('utf-8')
            except AttributeError:
                out[k] = v

    return out


def date_range(start, end, ascending=True, date_format='%Y-%m-%d'):
    """
    Simple datetime generator for dates between start and end (inclusive).

    :param start: Date to start at.
    :type start: datetime object or string representation of datetime.
    :param end: Date to stop at.
    :type end: datetime object or string representation of datetime.
    :param ascending: Toggle sorting of output.
    :type ascending: boolean
    :param date_format: If input is string, denotes string datetime format to convert from.
    :return: generator object for naive datetime objects
    """
    if isinstance(start, str):
        start_date = datetime.strptime(start, date_format)
    else:
        start_date = start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    if isinstance(end, str):
        end_date = datetime.strptime(end, date_format)
    else:
        end_date = end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    assert end_date >= start_date

    days_apart = (end_date - start_date).days + 1

    for i in (range(0, days_apart) if ascending else range(0, days_apart)[::-1]):
        yield start_date + timedelta(i)

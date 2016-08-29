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


def date_range(start, end, ascending=True):
    if isinstance(start, datetime):
        start_date = start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    else:
        start_date = datetime.strptime(start, '%Y-%m-%d')

    if isinstance(end, datetime):
        end_date = end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    else:
        end_date = datetime.strptime(end, '%Y-%m-%d')

    assert end_date >= start_date

    days_apart = (end_date - start_date).days + 1

    for i in (range(0, days_apart) if ascending else range(0, days_apart)[::-1]):
        yield start_date + timedelta(i)

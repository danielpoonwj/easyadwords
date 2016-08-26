from suds.sudsobject import asdict


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

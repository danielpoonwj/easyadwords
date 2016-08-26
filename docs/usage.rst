=====
Usage
=====

This package wraps the official googleads package to focus on simplifying the retrieval of Reports and Services through the API.

Authentication
--------------

To be granted access to AdWords' API, follow the steps listed here: https://developers.google.com/adwords/api/docs/guides/signup

Once you have received your developer token, create **googleads.yaml** in a secure location and fill in the following fields.

.. code-block:: yaml

    adwords:
      developer_token:
      user_agent:
      client_id:
      client_secret:
      refresh_token:
    # client_customer_id:

This credentials file would then be used to authenticate the AdwordsUtility.
Specifying *client_customer_id* in the file is optional, but it has to be either written explicitly in the file or input when creating the AdwordsUtility object.

.. code-block:: python

    from easyadwords import AdwordsUtility
    adwords_obj = AdwordsUtility(credential_path='path/to/credentials/googleads.yaml')

Caveats
-------

If you're using other packages that rely on oauth2client, googleads dependency specifies **oauth2client<2.0.0,>=1.5.2**.
You may have to update oauth2client separately if you're dependant on a more recent version.

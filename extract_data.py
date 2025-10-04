# #!/usr/bin/env python

from sodapy import Socrata

# # Unauthenticated client only works with public data sets. Note 'None'
# # in place of application token, and no username or password:
client = Socrata("data.sfgov.org", None)

def sample_api_data():
    # Filter by date directly in the API query using SoQL
    query = """
    SELECT * 
    WHERE requested_datetime  > "2025-01-01T00:00:00.000"
    LIMIT 45
    """

    results = client.get("vw6y-z8j6", query=query)
    return results
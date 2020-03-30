import requests
import os
import pandas as pd

from prefect import Flow, task
from typing import Any, List

# Define API key, endpoint, and the header
MY_API_KEY = os.getenv("MY_API_KEY")
ENDPOINT = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {MY_API_KEY}"}

# Define parameters
PARAMETERS = {"term": "coffee", "limit": 50, "radius": 10000, "location": "Plano"}

# Make a request to the yelp API
response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)

# Convert JSON string to a dictionary
business_data = response.json()
print(type(business_data["businesses"][0]))
business_data = business_data["businesses"]
# print(business_data["businesses"])
business_df = pd.DataFrame(business_data)
print(business_df)


@task
def get_business_data(offset: int) -> Any:
    parameter = {"term": "coffee", "limit": 50, "offset": offset, "radius": 10000, "location": "Plano"}
    # Make a request to the yelp API
    local_response = requests.get(url=ENDPOINT, params=parameter, headers=HEADERS)
    # Convert JSON string to a dictionary
    local_business_data = local_response.json()
    local_business_data = local_business_data["businesses"]
    local_business_df = pd.DataFrame(local_business_data)
    # return a data frame
    return local_business_df


@task
def reduce_to_one_data_frame(list_of_businesses: List) -> Any:
    merged_business_df = pd.concat(list_of_businesses, ignore_index=True)
    return merged_business_df


@task
def save_df_to_csv(businesses_df):
    businesses_df.to_csv("business.csv")


with Flow("getting yelp data") as flow:
    # Use mapping to get 500 businesses - 50 limit, 50 offset
    business_df_list = get_business_data.map([x for x in range(0, 500, 50)])
    reduced_businesses_df = reduce_to_one_data_frame(business_df_list)
    save_df_to_csv(reduced_businesses_df)

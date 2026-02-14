import pandas as pd
import numpy as np

from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@localhost:5432/university')


# read csv file into dataframe and also check the number of rows in the dataframe
df = pd.read_csv('bank_marketing.csv')
print(df.shape[0])


# replace '.' with '_' in the 'job' column
df['job'] = df['job'].str.replace('.','_')

# replace '.' with '_' in the 'marital' column
df['education'] = df['education'].str.replace('.', '_', regex=False)

# replace 'unknown' with NaN in the 'education' column
mask = df['education'].str.contains('unknown', case=False, na=False)
df.loc[mask, 'education'] = np.nan


# mapping values in the 'credit_default' column to 1 for 'yes', 0 for 'no' and 0 for 'unknown'
mapping = {'yes': 1, 'no': 0, 'unknown': 0}
df['credit_default'] = (
    df['credit_default']
    .str.lower()
    .replace(mapping)
    .astype(bool)
)


# mapping values in the 'mortgage' column to 1 for 'yes', 0 for 'no' and 0 for 'unknown'
mapping = {'yes': 1, 'no': 0, 'unknown': 0}
df['mortgage'] = (
    df['mortgage']
    .str.lower()
    .replace(mapping)
    .astype(bool)
)


# mapping values in the 'previous_outcome' column to 1 for 'success', 0 for 'failure' and 0 for 'nonexistent'
mapping = {'success': 1, 'failure': 0, 'nonexistent': 0}
df['previous_outcome'] = (
    df['previous_outcome']
    .str.lower()
    .replace(mapping)
    .astype(bool)
)


# mapping values in the 'campaign_outcome' column to 1 for 'yes' and 0 for 'no'
mapping = {'yes': 1, 'no': 0}
df['campaign_outcome'] = (
    df['campaign_outcome']
    .str.lower()
    .replace(mapping)
    .astype(bool)
)

# create a new column 'last_contact_date' by combining the 'month' and 'day' columns and converting it to datetime format
df['last_contact_date'] = pd.to_datetime(
    '2026 ' + df['month'].str.title() + ' ' + df['day'].astype(str),
    format='%Y %B %d',
    errors='coerce'
).dt.strftime('%Y-%m-%d')

# create three separate dataframes for client, campaign and economics data and save them as csv files
client = df[
    [
        'client_id',
        'age',
        'job',
        'marital',
        'education',
        'credit_default',
        'mortgage'
    ]
]

campaign = df[
    [
        'client_id',
        'number_contacts',
        'contact_duration',
        'previous_campaign_contacts',
        'previous_outcome',
        'campaign_outcome',
        'last_contact_date'
    ]
]

economics = df[
    [
        'client_id',
        'cons_price_idx',
        'euribor_three_months'
    ]
]
client.to_csv('client.csv' , index = False)
campaign.to_csv('campaign.csv' , index = False)
economics.to_csv('economics.csv' , index = False)


# load the three dataframes into the 'bronze' schema in the PostgreSQL database using the to_sql method
client.to_sql(
    'client',
    engine,
    schema='bronze',
    if_exists='append',
    index=False,
    
)

campaign.to_sql(
    'campaign',
    engine,
    schema='bronze',
    if_exists='append',
    index=False,
    
)
economics.to_sql(
    'economics',
    engine,
    schema='bronze',
    if_exists='append',
)


# read the data from the 'bronze' schema in the PostgreSQL database and print the first five rows of each table
data = pd.read_sql_query('SELECT * FROM bronze.economics', engine)
print(data.head())


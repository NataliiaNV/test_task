# test_task
test_Softconstruct


# 1.Import libraries

```python
import numpy as np 
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline

import warnings
warnings.filterwarnings('ignore')
```

# 2.Load data

#### Make clients
```python
credentials, your_project_id = google.auth.default()

bqclient = bigquery.Client(credentials=credentials, project=your_project_id,)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)
```
 

#### Define  query
```python
query_sales = "SELECT * FROM `bigquery-public-data.iowa_liquor_sales.sales` "
```

#### Getting data
##### I've also added the tqdm progress bar for seeing progress
```python
df = (
    bqclient.query(query_sales)
            .result()
            .to_dataframe(
                bqstorage_client=bqstorageclient,
                progress_bar_type='tqdm_notebook',)
)
```

# 3.Overview: basic understanding of our dataset

```python
df.head()
```

```python
df.info()
```

```python
df.describe().transpose()
```

# 4.Data cleaning


#### Displaying unique names of city

```python
print(df['city'].nunique())
print(df['city'].str.upper().nunique())

```

#### Displaying unique names of category
```python
print(df['category_name'].nunique())
print(df['category'].nunique())
print(df['category_name'].str.upper().nunique())
```
#### Making city,category_name and county to upper case
```python
df['city'] = df['city'].str.upper()
df['category_name'] = df['category_name'].str.upper()
df['county'] = df['county'].str.upper()
```
#### Checking for the Null values
```python
df.isna().sum()
```

#### Creating function for filling Null values
```python
def my_fillna( data,key, value):    
    subdata = data[[value,key]][data[key].isin(data[key][pd.isna(data[value])].unique())].dropna()
    dictionary = subdata.set_index(key).to_dict()[value]
    data[value] = data[value].fillna(data[key].apply(lambda x: dictionary.get(x)))
    return data[value]
```

#### Replacing Null values 
```python
df['city']=my_fillna(df,'store_number','city')
df['zip_code']= my_fillna(df,'store_number','zip_code')
df['address']= my_fillna(df,'store_number','address')
df['county_number']= my_fillna(df,'store_number','county_number')
df['county']= my_fillna(df,'store_number','county')
df['store_number']= my_fillna(df,'store_location','store_number')
df['county']= my_fillna(df,'store_number','county')
df['category_name']= my_fillna(df,'category','category_name')
```

#### Creating new columns for future analysis
```python
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df['day'] = df['date'].dt.day
```
#### Looking for other Null values and trying to replace them with information
```python
df[pd.isna(df.state_bottle_cost)]
```

```python
df.state_bottle_cost[df.item_number=='38180'].unique()
```

```python
df.year[pd.isna(df.state_bottle_cost)].unique()
```

```python
df[['item_number','year','state_bottle_cost']][df.item_number.isin(df.item_number[pd.isna(df.state_bottle_cost)])] \
.drop_duplicates().sort_values(by=['item_number','year'])
```

```python
df[['item_number','year','state_bottle_retail']][df.item_number.isin(
    (df.item_number[pd.isna(df.state_bottle_retail)]))] \
.drop_duplicates().sort_values(by=['item_number','year'])
```

```python
df['state_bottle_cost'][df.year ==2012] = my_fillna(df[df.year ==2012],'item_number','state_bottle_cost')
df['state_bottle_retail'][df.year ==2012]= my_fillna(df[df.year ==2012],'item_number','state_bottle_retail')
```

#### Checking for the Null values again
```python
df.isna().sum()
```

```python
df.store_number[pd.isna(df.city)].unique()
```
```python
df.city[df.store_number=='5320'].unique()
```
```python
df[pd.isna(df.sale_dollars)]
```
```python
df.sale_dollars = df.sale_dollars.fillna(df.state_bottle_retail*df.bottles_sold)
```
#### For avoiding delete Null values
```python
df.city = df.city.fillna('NOT_FOUND')
```

```python
df.isna().sum()
```
#### Look on data correlation, it's expecting result to see big corr for volumes and prices 
```python
sns.heatmap(df.corr(), annot=True, fmt=".1f")
```
#### Top products. Here we can see strange data for last positions, receiving income without sale volumes alcohol. But I have not data for fixing(can't find right prices).
```python
df.groupby(['item_description'],as_index=False) \
        .agg({'volume_sold_liters':'sum' ,'volume_sold_gallons':'sum' ,  'sale_dollars':'sum'}) \
        .sort_values(['volume_sold_liters', 'sale_dollars' ], ascending=False) \
        .astype({'sale_dollars': 'int','volume_sold_liters': 'int','volume_sold_gallons': 'int'})
```
```python
df.item_description[(df.volume_sold_liters==0) & (df.sale_dollars!=0)].unique()
```
```python
df0 = df[['item_description','state_bottle_retail','year','bottle_volume_ml','month']] \
[df.state_bottle_retail==0].drop_duplicates().sort_values('item_description', ascending=False)
```

```python
df1 = df[['item_description','state_bottle_retail','year','bottle_volume_ml','month']] \
[df.item_description.isin(df.item_description[df.state_bottle_retail==0].unique())] \
.drop_duplicates().sort_values('item_description', ascending=False)
```



```python
df0 \
    .merge(df1, on=['item_description','year','bottle_volume_ml']) \
    .query("state_bottle_retail_y!=0") \
    .drop_duplicates()
```

#### But we can fix 'sale_dollars' where sale_dollars and state_bottle_retail not 0

```python
df[(df.volume_sold_liters!=0) & (df.sale_dollars==0)]
```
```python
df[ (df.sale_dollars==0) & (df.state_bottle_retail!=0)]
```

```python
df.sale_dollars[ (df.sale_dollars==0) & (df.state_bottle_retail!=0) & \
                (df.bottles_sold!=0)] = df.state_bottle_retail*df.bottles_sold
```
#### Also we can find bottle_volume_ml
```python
df[['item_description','bottle_volume_ml']][df.item_description.isin(df.item_description[(df.bottle_volume_ml==0) 
                                    & (df.sale_dollars!=0)].unique())].drop_duplicates()
```
```python
df = df.drop(df[df.item_description=="Canadian Club Dock 57 Mini DNO"].index)
```
```python
df.volume_sold_liters[df.volume_sold_liters==0]=df.bottle_volume_ml*df.bottles_sold/1000
```
```python
df.bottle_volume_ml[(df.item_description=="Burnett's Pink Lemonade Vodka Mini")  &(df.bottle_volume_ml==0) ]=500
df.bottle_volume_ml[(df.item_description=="Three Olives Orange Vodka")  &(df.bottle_volume_ml==0) ]=750
df.bottle_volume_ml[(df.item_description=="Rondiaz DSS Lemon Rum")  &(df.bottle_volume_ml==0) ]=1000
```

# 5.Basic information from data

#### Top products per year
```python
df.groupby(['year','item_description'],as_index=False) \
        .agg({'volume_sold_liters':'sum' , 'sale_dollars':'sum'}) \
        .sort_values(['year','volume_sold_liters', 'sale_dollars' ], ascending=False) \
        .astype({'sale_dollars': 'int'})
```
#### We can see a trend of sales growth, but in 2020 quarantine changed the statistics
```python
df.groupby(['year','item_description'],as_index=False) \
    .agg({'volume_sold_liters':'sum' , 'sale_dollars':'sum'}) \
    .sort_values(['volume_sold_liters', 'sale_dollars' ], ascending=[False,False]) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('year') \
    .head(1)
```
#### The most popular item every year is Black Velvet
```python
df.groupby(['year','item_description'],as_index=False) \
    .agg({'volume_sold_liters':'sum' , 'sale_dollars':'sum'}) \
    .sort_values(['volume_sold_liters', 'sale_dollars' ], ascending=[False,False]) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('year') \
    .head(1)
```
#### New Amsterdam Orange Mini doesn't bring money in 2020, I think stop transporting is a good idea
```python
df_problem_product = df.groupby(['year','item_description'],as_index=False) \
    .agg({'volume_sold_liters':'sum' , 'sale_dollars':'sum'}) \
    .sort_values(['volume_sold_liters', 'sale_dollars' ], ascending=[True,True]) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('year') \
    .head(1)

df_problem_product
```

```python
df[df.item_description.isin(df_problem_product.item_description)] \
        .groupby(['year','item_description'],as_index=False) \
        .agg({'volume_sold_liters':'sum','sale_dollars':'sum'}) \
        .sort_values(['item_description','year'], ascending=[True,True]) \
        .astype({'sale_dollars': 'int'})

```
#### "COLORADO SPRINGS" not a city of alcohol lovers if the most pupular item bring only 15$
```python
df.groupby(['city','item_description'],as_index=False) \
    .agg({'volume_sold_liters':'sum' , 'sale_dollars':'sum'}) \
    .sort_values(['volume_sold_liters', 'sale_dollars' ], ascending=[False,False]) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('city') \
    .head(1)
```


#### Let's look at graphic of most income-generating category of alcohol and their volume. # Here we can see that vodka is 3d on volume and 5th on income
```python
df_plot = df.groupby(['category_name'],as_index=False) \
    .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum'})  \
    .sort_values('sale_dollars', ascending =False) \
    .astype({'sale_dollars': 'int'})
```

```python
import plotly.express as px

fig_reg = px.bar(df_plot.head(10),x='category_name', y='volume_sold_liters',color='volume_sold_liters')
fig_reg.update_layout(
    title="Volume of sold liquor per category",
    xaxis_title=" Category Name",
    yaxis_title="Volume in liters",
    )

fig_reg2 = px.bar(df_plot.head(10),x='category_name', y='sale_dollars',color='volume_sold_liters')
fig_reg2.update_layout(
    title="Sales of liquor per category",
    xaxis_title="Category Name",
    yaxis_title="Sales in dollars",
    )


fig_reg.show()
fig_reg2.show()
```
#### Favorite drink for every city
```python
df.groupby(['city','item_description'],as_index=False) \
    .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum', 'bottles_sold':'sum', 'state_bottle_retail':'mean'}) \
    .sort_values(['sale_dollars'], ascending =False) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('city') \
    .head(1)
```

#### In 'DES MOINES' new favorite drink "Hennessy VS"
```python
df[df.city=='DES MOINES'].groupby(['year','item_description'],as_index=False) \
    .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum', 'bottles_sold':'sum', 'state_bottle_retail':'mean'}) \
    .sort_values(['sale_dollars'], ascending =False) \
    .astype({'sale_dollars': 'int'}) \
    .groupby('year') \
    .head(1)
```

#### The largest sales of alcohol are in 12 and 10 months
```python
df.groupby(['month'],as_index=False) \
        .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum', 'bottles_sold':'sum', 'state_bottle_retail':'mean'}) \
    .sort_values(['sale_dollars'], ascending =False) \
    .astype({'sale_dollars': 'int','volume_sold_liters': 'int'}) 
```

#### There seems alcohol sell the most Monday - Thursday, and see sales fall Friday and Sunday.
```python
dict_day = {0:"Monday",1:"Tuesday",2:"Wednesday",3:"Thursday",4:"Friday",5:"Saturday",6:"Sunday"}
df['week_day'] = df.date.dt.dayofweek.apply(lambda x: dict_day[x])

df.groupby(['week_day'] ,as_index=False) \
        .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum', 'bottles_sold':'sum',  'state_bottle_retail':'mean'}) \
    .sort_values(['sale_dollars'], ascending =False) \
    .astype({'sale_dollars': 'int','volume_sold_liters': 'int'}) 
```
####  We can see a trend of sales growth, but in 2020 quarantine changed the statistics
```python
df_year = df.groupby(['year'] ,as_index=False) \
    .agg({'sale_dollars':'sum', 'volume_sold_liters':'sum', 'bottles_sold':'sum',  'state_bottle_retail':'mean'}) \
    .sort_values(['volume_sold_liters'], ascending =False) \
    .astype({'sale_dollars': 'int','volume_sold_liters': 'int'})
df_year
```
```python
plt.figure(figsize=(12,5))
sns.barplot(x='year',y='sale_dollars',data=df_year)
```

### So, this is basic information from the data, of course, we can choose the direction and develop it, depending on the problem we want to solve.



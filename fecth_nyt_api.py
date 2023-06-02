from datetime import date, datetime
import datetime
from pynytimes import NYTAPI
import pandas as pd
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import time

apikey = "JhhAI9HLHKkNsIP2pwe0OaUR0APO46WX"
nytapi = NYTAPI(apikey, parse_dates=True)


#Get today's date
today = datetime.datetime.now().date()

# Subtract 2 day from today
begin_date = today - datetime.timedelta(days=2)

# Set end_date 
end_date = today - datetime.timedelta(days=1)

articles = nytapi.article_search(
    query = "artificial intelligence", # Search for articles about artificial intelligence
    results = 100, 
    # Search for articles in range date
    dates = {
       "begin": datetime.datetime(begin_date.year, begin_date.month, begin_date.day),
       "end": datetime.datetime(end_date.year, end_date.month, end_date.day)
    },
    options = {
        "sort": "newest", # Sort by newest options
    }
)
#print how many article that we got
print(len(articles))


# for each of the articles in the list, get the information that is stored in a nested dictionary:
abstract = map(lambda x: x["abstract"], articles)
web_url = map(lambda x: x["web_url"], articles)
snippet = map(lambda x: x["snippet"], articles)
lead_paragraph = map(lambda x: x["lead_paragraph"], articles)
source = map(lambda x: x.get("source", ""), articles)
pub_date = map(lambda x: x["pub_date"], articles)
document_type = map(lambda x: x["document_type"], articles)
news_desk = map(lambda x: x["news_desk"], articles)
section_name = map(lambda x: x.get("section_name", ""), articles)
type_of_material = map(lambda x: x.get("type_of_material", ""), articles)
word_count = map(lambda x: x["word_count"], articles)
article_id = map(lambda x: x["_id"], articles)
subsection_name = map(lambda x: x.get("subsection_name", ""), articles)
headline = map(lambda x: x["headline"]["main"], articles)
author = map(lambda x: x["byline"]["original"], articles)

# since keywords are a branch down in the nested dictionary, we need to add an additional for loop to collect all keywords:
keywords = map(lambda x:list(i["value"] for i in x["keywords"]), articles)

data={'id': list(article_id),
      'publication_date': list(pub_date),
      'web_url': list(web_url),
      'headline': list(headline),
      'source': list(source),
      'author': list(author),  
      'snippet': list(snippet),
      'lead_paragraph': list(lead_paragraph),
      'abstract': list(abstract),
      'document_type': list(document_type),
      'news_desk': list(news_desk),
      'section_name': list(section_name),
      'subsection_name': list(subsection_name),
      'type_of_material': list(type_of_material),
      'keywords': list(keywords),
      'word_count': list(word_count)
      }

# Create the DataFrame 
df = pd.DataFrame(data)

# Convert publication_date to string format
df['publication_date'] = df['publication_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Write the DataFrame to Parquet file using pyarrow
new_data = pa.Table.from_pandas(df)

#define current date
date = str(date.today())

#save new data
pq.write_table(new_data, f'/home/ahyar/final_project/NYT_{date}.parquet')

#load existing table
existing_data = pq.read_table('/home/ahyar/final_project/artificial-intelligence_Jan-Mei_copy.parquet')

#concat to existing table
data_concat = pa.concat_tables([existing_data, new_data])

# Write the updated table to the Parquet file
pq.write_table(data_concat, '/home/ahyar/final_project/artificial-intelligence_Jan-Mei_copy.parquet')
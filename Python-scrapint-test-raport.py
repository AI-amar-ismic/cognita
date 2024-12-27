# Databricks notebook source
# MAGIC %pip install requests beautifulsoup4 azure-ai-textanalytics pyspark datetime python-dateutil

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from datetime import datetime, timedelta
from dateutil import parser

def scrape_search_results(url):
    try:
        articles = []
        thirty_days_ago = datetime.now().date() - timedelta(days=30)
        while url:
            # Send a GET request to the search results page
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            articles_container = soup.find('ul', class_='teasers')
        
            for article_tag in articles_container.find_all('div', class_='teaser-content'):  # Adjust the class based on the site's structure
                title_tag = article_tag.find('a', class_=False)
                link_tag = title_tag.find('h3') if title_tag else None
                date_tag = article_tag.find('time', class_=False)
                print(date_tag['datetime'])
                if link_tag and date_tag:
                    article_date = parser.isoparse(date_tag['datetime']).date()
                    # Stop scraping if the article is older than 30 days
                    if article_date < thirty_days_ago:
                        return articles
                    articles.append({
                        'title': link_tag.get_text(strip=True),
                        'url': title_tag['href']
                    })
            next_page_tag = soup.find('a', class_='next')  # Adjust class based on the site's structure
            url = next_page_tag['href'] if next_page_tag else None
        return articles
    except Exception as e:
        print(f"An error occurred while scraping search results: {e}")
        return []
    
def scrape_article(url):
    try:
        # Send a GET request to the article URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract the title
        title = soup.find('h1', class_='article-title').get_text(strip=True)

        # Extract the date
        date = soup.find('time', class_=False).get('datetime')

        # Extract the text of the article
        article_body = soup.find('article', class_='article')
        paragraphs = article_body.find_all('p', class_=False)  # Only <p> tags without any class names
        text = "\n".join(p.get_text(strip=True) for p in paragraphs)

        return {
            'title': title,
            'date': date,
            'text': text,
            'url': url
        }
    except Exception as e:
        print(f"An error occurred while scraping article: {e}")
        return None

def authenticate_client():
    # Replace with your Azure Text Analytics endpoint and key
    endpoint = "https://cognita-sentiment.cognitiveservices.azure.com/"
    key = "EpLnnvyDMXsNH7MG708J075a84B1lmqczjAGbpj4aAykEfaiOnisJQQJ99ALAC5RqLJXJ3w3AAAaACOGDrlh"
    return TextAnalyticsClient(endpoint=endpoint, credential=AzureKeyCredential(key))

def analyze_sentiment(client, document):
    try:
        response = client.analyze_sentiment(documents=[document])[0]
        return response
    except Exception as e:
        print(f"An error occurred while analyzing sentiment: {e}")
        return None

def store_data_in_sql(spark, data):
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("url", StringType(), True),
        StructField("overall_sentiment", StringType(), True),
        StructField("positive_score", FloatType(), True),
        StructField("negative_score", FloatType(), True),
        StructField("neutral_score", FloatType(), True)
    ])

    df = spark.createDataFrame([data], schema=schema)
    df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("sentiment_analysis_results")
    

# Example usage
if __name__ == "__main__":
    search_url = "https://raport.ba/?s=muzur"
    articles = scrape_search_results(search_url)

    if articles:
        # Authenticate Azure client
        client = authenticate_client()

        # Initialize Spark session
        spark = SparkSession.builder.appName("CognitaSentiment").getOrCreate()

        for article in articles:
            article_data = scrape_article(article['url'])

            if article_data:
                # Analyze sentiment of the article text
                sentiment_response = analyze_sentiment(client, article_data['text'])

                if sentiment_response:
                    print("Sentiment Analysis Result:")
                    print(f"Overall Sentiment: {sentiment_response.sentiment}")
                    print(f"Confidence Scores: Positive={sentiment_response.confidence_scores.positive}, Negative={sentiment_response.confidence_scores.negative}, Neutral={sentiment_response.confidence_scores.neutral}")

                    # Prepare data for storage
                    sentiment_data = {
                        "title": article_data['title'],
                        "date": article_data['date'],
                        "text": article_data['text'],
                        "url": article_data['url'],
                        "overall_sentiment": sentiment_response.sentiment,
                        "positive_score": sentiment_response.confidence_scores.positive,
                        "negative_score": sentiment_response.confidence_scores.negative,
                        "neutral_score": sentiment_response.confidence_scores.neutral
                    }

                    # Store data in SQL
                    store_data_in_sql(spark, sentiment_data)


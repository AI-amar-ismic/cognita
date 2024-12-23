# Databricks notebook source
# MAGIC %pip install requests beautifulsoup4

# COMMAND ----------

import requests
from bs4 import BeautifulSoup

def scrape_article(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        article_body = soup.find('article', class_='article')
        paragraphs = article_body.find_all('p', class_=False)  # Only <p> tags without any class names
        text = "\n".join(p.get_text(strip=True) for p in paragraphs)


        # Return the extracted data
        return {
            'text': text
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

# Example usage
if __name__ == "__main__":
    url = "https://raport.ba/trojka-na-sastanku-postavila-temelje-za-rekonstrukciju-vlade-ks-muzur-spreman-preuzeti-premijersku-poziciju-u-cetvrtak-novi-sastanak/"
    article_data = scrape_article(url)

    if article_data:
        print("Title:", article_data['text'])


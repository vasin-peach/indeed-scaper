import multiprocessing as mp
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse


def main():


  POOL = mp.Pool(mp.cpu_count())
  KEYWORD = pd.read_csv("data/jobs_position_category.csv", header=None, encoding='cp1252')

  base_url = "https://www.indeed.com/"
  pages = 30
  pool = mp.Pool(mp.cpu_count())

  # # scap job url
  # result = pool.starmap_async(run_scap, [(keyword, pages, base_url) for keyword in KEYWORD[0]]).get()
  # pool.close()

  # # scap job data
  job_link = pd.read_csv("data/job_url.csv", header=None)
  result = pool.map_async(run_scap2, [link for link in job_link[1]]).get()
  pool.close()


def request_query_soup(keyword, page, base_url):
  query_search = f"{base_url}jobs?q={urllib.parse.quote(keyword)}&start={page}"
  return request_query(query_search)

def request_query(page):
  response = requests.get(page)
  soup = BeautifulSoup(response.text, "html.parser")
  return soup

def run_scap(keyword='Web Developer', pages=1, base_url='https://www.indeed.com/'):
  print(keyword)
  for page in range(pages):
      page = page * 10
      soup = request_query_soup(keyword, page, base_url)

      link_list = []
      keyword_list = []

      query_div = soup.find_all("div", {"class": "jobsearch-SerpJobCard"})
      for div in query_div:
          link_list.append(f"https://www.indeed.com/viewjob?jk={div.attrs['data-jk']}&from=serp&vjs=3")
          keyword_list.append(keyword)

      df = pd.DataFrame({'keyword': keyword_list, 'url': link_list})
      df.to_csv('data/job_url.csv', mode='a', header=False, index=False)


def run_scap2(link):
  print(link)
  soup = request_query(link)
  header = soup.find("h3", {"class": "jobsearch-JobInfoHeader-title"})
  header = header.getText() if header else ""
  company = soup.find("div", {"class": "jobsearch-InlineCompanyRating"})
  company = company.getText() if company else ""
  desc = soup.find("div", {"class": "jobsearch-jobDescriptionText"})
  desc = desc.getText() if desc else ""

  df = pd.DataFrame({'job_title': [header], 'company': [company], 'desc': [desc]})
  df.to_csv('data/job_scrap.csv', mode='a', header=False, index=None)

if __name__ == '__main__': 
  main()
  

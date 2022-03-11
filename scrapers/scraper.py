import csv
from pathlib import Path
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import difflib

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent
BASE_URL = "https://www.newsvoir.com"

def get_driver(headless):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")

    # initialize driver
    driver = webdriver.Chrome(chrome_options=options)
    return driver


def connect_to_base_url(browser, base_url=None):
    if base_url is None:
        base_url = BASE_URL
    connection_attempts = 0
    while connection_attempts < 3:
        try:
            browser.get(base_url)
            # wait for table element with id = 'content' to load
            # before returning True
            WebDriverWait(browser, 5).until(
                EC.presence_of_element_located((By.ID, "home-latest"))
            )
            return base_url
        except Exception as e:
            print(e)
            connection_attempts += 1
            print(f"Error connecting to {base_url}.")
            print(f"Attempt #{connection_attempts}.")
    return False


def parse_base_html(html, base_url=None):
    if base_url is None:
        base_url = BASE_URL
    # create soup object
    soup = BeautifulSoup(html, "html.parser")
    tags = {tag.name for tag in soup.find_all()}
    div_key = 'div'
    tag = difflib.get_close_matches(div_key, tags, n=1)
    tag = tag and tag[0]
    div_tags = soup.find_all(tag)
    category_links = []
    for div_tag_number, div_tag in enumerate(div_tags):
        attrs = div_tag.attrs
        if 'class' in attrs.keys() and "rightColumn" in attrs['class'] and "right-wrapper" in attrs['class'] and attrs['align'] == 'right' and attrs['valign'] == 'top':
            try:
                table_soup = div_tag.table.tbody.tr.td.table.tbody
            except AttributeError:
                continue
            for td_soup in table_soup.find_all():
                if 'class' in td_soup.attrs and 'ncat' in td_soup.attrs['class']:
                    li_list = td_soup.find_all('li')
                    for li_item in li_list:
                        anker = li_item.a
                        if "href" in anker.attrs:
                            url = base_url + anker.attrs["href"]
                            text = anker.text
                            cat_link_dict = {text:url}
                            if cat_link_dict not in category_links:
                                category_links.append(cat_link_dict)
    return category_links


def parse_child_html(html, base_url=None):
    if base_url is None:
        base_url = BASE_URL
    # create soup object
    soup = BeautifulSoup(html, "html.parser")
    tags = {tag.name for tag in soup.find_all()}
    div_key = 'div'
    tag = difflib.get_close_matches(div_key, tags, n=1)
    tag = tag and tag[0]
    div_tags = soup.find_all(tag)
    category_child_links = []
    for div_tag_number, div_tag in enumerate(div_tags):
        attrs = div_tag.attrs
        if 'class' in attrs.keys() and "table-work" in attrs['class']:
            try:
                table_soup = div_tag.table.tbody
            except AttributeError:
                continue
            tags = {tag.name for tag in table_soup.find_all()}
            div_key = 'div'
            tag = difflib.get_close_matches(div_key, tags, n=1)
            tag = tag and tag[0]
            table_div_tags = table_soup.find_all(tag)
            for table_div_tag in table_div_tags:
                if 'id' in table_div_tag.attrs and table_div_tag.attrs['id']=='more_release':
                    for td_table in table_div_tag.find_all('td'):
                        if 'class' in td_table.attrs and 'category-waste' in td_table.attrs['class'] and 'waste' in td_table.attrs['class']:
                            anker = td_table.a
                            if "href" in anker.attrs:
                                url = base_url[:base_url.find('.com')+len('.com')] + anker.attrs["href"]
                                text = anker.text
                                text_link_dict = {text:url}
                                if text_link_dict not in category_child_links:
                                    category_child_links.append(text_link_dict)
    return category_child_links


def parse_html_for_content(html, join_string_by=''):
    soup = BeautifulSoup(html, "html.parser")
    location = None
    date_string = None
    content = []
    bold_content = []
    for div_tag in soup.find_all('div'):
        attrs = div_tag.attrs
        if 'class' in attrs.keys() and "coments-main" in attrs['class']:
            if 'class' in div_tag.div.attrs and "aug" in div_tag.div.attrs['class']:
                match = re.search("['0-9A-Za-z :,']+", div_tag.div.text)
                date_string = match.string[match.span()[0]:match.span()[1]].strip()
    for td_tag in soup.find_all('td'):
        attrs = td_tag.attrs
        if 'class' in attrs.keys() and "toronto-text" in attrs['class'] and "article-body-res" in attrs['class']:
            location = td_tag.b.text
            paragarphs = td_tag.find_all('p')
            for paragarph in paragarphs:
                strong_phrase = paragarph.strong
                strong_phrase and bold_content.append(strong_phrase.text)
                text = paragarph.text
                if text and re.search("['0-9A-Za-z ']+", text):
                    content.append(text)
    content = join_string_by.join(content)
    return content, date_string , location


def get_pagination_index(html):
    soup = BeautifulSoup(html, "html.parser")
    div_tags = soup.find_all("div")
    page_numbers = []
    for div_tag in div_tags:
        if "class" in div_tag.attrs and 'pagination' in div_tag.attrs['class']:
            for anker in div_tag.find_all('a'):
                try:
                    page_number = int(anker.text)
                    page_numbers.append(page_number)
                except ValueError:
                    continue
    if page_numbers:
        start = min(page_numbers) - 1
    else:
        start = 0
    if page_numbers:
        end = max(page_numbers) + 1
    else:
        end = 0
    return start, end


def load_next_page(article_url):
    try:
        # set headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"
        }
        # make get request to article_url
        response = requests.get(
            article_url, headers=headers, stream=True, timeout=3.000
        )
        # Beautiful soup
        soup = BeautifulSoup(response.text)
    except Exception as e:
        print(e)
        soup = None
    return soup


def get_load_time(article_url):
    try:
        # set headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36"
        }
        # make get request to article_url
        response = requests.get(
            article_url, headers=headers, stream=True, timeout=3.000
        )
        # get page load time
        load_time = response.elapsed.total_seconds()
    except Exception as e:
        print(e)
        load_time = "Loading Error"
    return load_time


def write_to_file(output_list, filename):
    for row in output_list:
        with open(Path(BASE_DIR).joinpath(filename), "a") as csvfile:
            fieldnames = ["url", "title", "last_modified"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerow(row)

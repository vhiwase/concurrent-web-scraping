import datetime
import sys
import os
from time import sleep, time
import pandas as pd
import pathlib
from collections import Counter
import string
try:
    basedir = pathlib.os.path.abspath(pathlib.os.path.dirname(__file__))
except NameError:
    basedir = pathlib.os.path.abspath(pathlib.os.path.dirname("."))


try:
    from scrapers.scraper import connect_to_base_url, get_driver, parse_base_html, parse_child_html, get_pagination_index, parse_html_for_content
except ModuleNotFoundError:
    pathlib.sys.path.insert(0, basedir)
    from scrapers.scraper import connect_to_base_url, get_driver, parse_base_html, parse_child_html, get_pagination_index, parse_html_for_content


def get_cat_link_dict(filename, browser):
    if connect_to_base_url(browser):
        sleep(2)
        html = browser.page_source
        output_list = parse_base_html(html)
        return output_list 
    else:
        print("Error connecting to NewsVoir")
        

def get_content_from_link(link, browser):
    if connect_to_base_url(browser, base_url=link):
        sleep(2)
        html = browser.page_source
        content, date_string , location, bold_content, source_link_text, source_link = parse_html_for_content(html, join_string_by="__:paragraph-seperator:__")
        return content, date_string , location, bold_content, source_link_text, source_link


def save_categories(filename, browser):
    output_list = get_cat_link_dict(filename, browser)
    cat_child_links_dict = {}
    for enum, cat_link in enumerate(output_list):
        for cat, link in cat_link.items():
            cat_child_links = []
            if connect_to_base_url(browser, base_url=link):
                sleep(2)
                html = browser.page_source
                # Deciding start page and end page
                start, end = get_pagination_index(html)
                for page_num in range(start, end):
                    base_url = "{0}&page={1}".format(link, page_num)
                    if connect_to_base_url(browser, base_url=base_url):
                        sleep(2)
                        html_page = browser.page_source
                        category_child_links = parse_child_html(html_page, base_url=base_url)
                        for item in category_child_links:
                            if item not in cat_child_links:
                                cat_child_links.append(item)
                    print("Yaahoo '{}' page{} out of page{} are completed! but yaar '{}' categories are still pending ...".format(cat, page_num, end, len(output_list)-enum))
                print("'{}' category parsing completed successfully! Let's have a small celebration ...".format(cat))
            else:
                print("'{}' something went wrong".format(cat))
            print("----------")
            print()
            kk_list = []
            vv_list = []
            for item in cat_child_links:
                for kk, vv in item.items():
                    kk_list.append(kk)
                    vv_list.append(vv)                    
            dd = pd.DataFrame()
            dd['title'] = kk_list
            dd['link'] = vv_list
            temp_folder = pathlib.Path(basedir) / 'output_categories'
            pathlib.os.makedirs(temp_folder, exist_ok=True)
            cat_name = cat.strip()
            for s in string.punctuation:
                cat_name = cat_name.replace(s, '_')
            cat_name = '_'.join(cat_name.split())
            cat_csv_path = temp_folder/ "{}.csv".format(cat_name)
            cat_child_links_dict[cat] = cat_child_links
            dd.to_csv(cat_csv_path)
    return cat_child_links_dict
            

def run_process(filename, browser):
    category_list = []
    title_list = []
    link_list = []
    content_list = []
    date_string_list = []
    location_list = []
    bold_content_list = []
    source_link_text_list = []
    source_link_list = []
    category_folder = (pathlib.Path(basedir) / 'categories')
    if not category_folder.is_dir():
        cat_child_links_dict = save_categories(filename, browser)
        for key, values in cat_child_links_dict.items():
            for value in values:    
                category_list.append(key)
                for title, link in value.items():
                    title_list.append(title)
                    link_list.append(link)
    else:
        for csv_file in os.listdir(category_folder):
            csv_path = category_folder/csv_file
            csv_path = csv_path.as_posix()
            df = pd.read_csv(csv_path, index_col=[0])
            title_list.extend(df.title.tolist())
            link_list.extend(df.link.tolist())
            category_list.append(csv_file.replace('.csv', ''))
    category_counter = dict(Counter(category_list))
    count = 0
    for c, link in enumerate(link_list):
        content, date_string , location, bold_content, source_link_text, source_link = get_content_from_link(link, browser)
        content_list.append(content)
        date_string_list.append(date_string)
        location_list.append(location)
        bold_content_list.append(bold_content)
        source_link_text_list.append(source_link_text)
        source_link_list.append(source_link)
        total_length = len(link_list)-c
        remaining_length = category_counter[category_list[c]]
        category_counter[category_list[c]] -= 1
        print("{}: {} file(s) remaining...\t\tTotal {} file(s) remaining ...\n".format(category_list[c], remaining_length, total_length))
        if count%100==0:
            df = pd.DataFrame()
            df['category'] = category_list[:len(content_list)]
            df['title'] = title_list[:len(content_list)]
            df['link'] = link_list[:len(content_list)]
            df['content'] = content_list
            df['date_string'] = date_string_list
            df['location'] = location_list
            df['bold_content'] = bold_content_list
            df['source_link_text'] = source_link_text_list
            df['source_link'] = source_link_list
            
            temp_folder = pathlib.Path(basedir) / 'output_temp_files'
            pathlib.os.makedirs(temp_folder, exist_ok=True)
            csv_path = temp_folder/ "df_{}.csv".format(count)
            if isinstance(csv_path , pathlib.Path):
                csv_path = csv_path.as_posix()
            df.to_csv(csv_path)
        count += 1
    print()
    print('-'*100)
    print("Remaining counter dictionary:", category_counter)
    print('-'*100)
    print()
    df = pd.DataFrame()
    df['category'] = category_list[:len(content_list)]
    df['title'] = title_list[:len(content_list)]
    df['link'] = link_list[:len(content_list)]
    df['content'] = content_list
    df['date_string'] = date_string_list
    df['location'] = location_list
    df['bold_content'] = bold_content_list
    df['source_link_text'] = source_link_text_list
    df['source_link'] = source_link_list
    df.to_csv(filename)
    if len(title_list) == len(content_list):
        print("NewsVoir Webscraping completed successfully")
    else:
        print("NewsVoir Webscraping completed but there are some problems with some files. Please take a look.")



if __name__ == "__main__":

    # headless mode?
    headless = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "headless":
            print("Running in headless mode")
            headless = True

    # set variables
    start_time = time()
    current_attempt = 1
    output_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"output_{output_timestamp}.csv"

    # init browser
    browser = get_driver(headless=headless)

    # scrape and crawl
    print(f"Scraping NewsVoir #{current_attempt} time(s)...")
    run_process(output_filename, browser)

    # exit
    browser.quit()
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time: {elapsed_time} seconds")

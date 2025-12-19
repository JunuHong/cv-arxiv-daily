import os
import re
import json
import arxiv
import yaml
import logging
import argparse
import datetime
from typing import Optional
import requests
from requests.exceptions import RequestException, SSLError
import urllib3
from importlib.metadata import PackageNotFoundError, version

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

PWC_API_BASE_URLS = (
    "https://paperswithcode.com/api/v0/papers/",
    # Legacy hostname kept as a fallback in case the primary endpoint changes again.
    "https://arxiv.paperswithcode.com/api/v0/papers/",
)
DEFAULT_HEADERS = {
    "User-Agent": "cv-arxiv-daily-bot/1.0 (+https://github.com/binary-husky/cv-arxiv-daily)"
}
CODE_LINK_HOST_PREFIXES = (
    "https://github.com",
    "https://huggingface.co",
    "https://dagshub.com",
    "https://catalyzex.com",
    "https://www.catalyzex.com",
    "https://alphaxiv.org",
    "https://paperswithcode.com",
)
arxiv_url = "http://arxiv.org/"
REQUEST_TIMEOUT = 5
ARXIV_CLIENT = arxiv.Client(page_size=50, delay_seconds=3, num_retries=3)
try:
    ARXIV_LIBRARY_VERSION = version("arxiv")
except PackageNotFoundError:
    ARXIV_LIBRARY_VERSION = "unknown"

SUPPORTED_REPO_PREFIXES = (
    "https://github.com",
    "https://huggingface.co",
)


def fetch_official_repo(paper_id: str) -> Optional[str]:
    """Fetch the official code repository for a paper.

    Tries multiple request strategies to tolerate SSL handshake issues before
    ultimately returning ``None`` on failure or missing repository info.
    """

    # Avoid noisy warnings when we fall back to requests without certificate
    # verification. Requests already emits a warning of its own, so silence the
    # urllib3 layer here.
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    candidate_urls = [url + paper_id for url in PWC_API_BASE_URLS]
    candidates = []
    for target in candidate_urls:
        candidates.append((target, {}))
        candidates.append((target, {"verify": False}))

    for target_url, request_kwargs in candidates:
        try:
            response = requests.get(
                target_url,
                timeout=REQUEST_TIMEOUT,
                headers=DEFAULT_HEADERS,
                **request_kwargs,
            )
            response.raise_for_status()
            data = response.json()
            if "official" in data and data["official"]:
                return data["official"].get("url")
        except SSLError as exc:
            logging.warning(
                "SSL error while fetching code link for %s (%s): %s",
                paper_id,
                target_url,
                exc,
            )
            continue
        except RequestException as exc:
            logging.error(
                "Request failed for %s (%s): %s", paper_id, target_url, exc
            )
            continue
        except ValueError as exc:
            logging.error("Invalid JSON for %s (%s): %s", paper_id, target_url, exc)
            continue
    return None


def _is_supported_repo_url(url: Optional[str]) -> bool:
    return bool(url) and url.startswith(SUPPORTED_REPO_PREFIXES)


def _extract_code_link_from_arxiv_links(links) -> Optional[str]:
    """Return the first supported code/data link exposed by arXiv."""

    for link in links or []:
        href = getattr(link, "href", "") or ""
        if not href:
            continue
        if _is_supported_repo_url(href):
            return href
        if href.startswith(CODE_LINK_HOST_PREFIXES):
            return href
    return None


def _fetch_single_arxiv_result(paper_id: str) -> Optional[arxiv.Result]:
    search_engine = arxiv.Search(id_list=[paper_id], max_results=1)
    for result in iter_search_results(search_engine):
        return result
    return None


_ARXIV_RESULT_MISSING = object()


def find_code_repository(
    paper_id: str,
    paper_title: str,
    arxiv_result: Optional[arxiv.Result] | object = _ARXIV_RESULT_MISSING,
) -> Optional[str]:
    """Find a code repository link for a paper.

    Prefer links surfaced directly by arXiv (CatalyzeX/HuggingFace/DagsHub/Papers
    with Code widgets). Fallback to the Papers with Code API as a last resort.
    """

    # Important: callers may *intentionally* pass `None` to indicate we've already
    # fetched and there was no arXiv result. Don't refetch in that case.
    if arxiv_result is _ARXIV_RESULT_MISSING:
        result = _fetch_single_arxiv_result(paper_id)
    else:
        result = arxiv_result
    if result:
        repo_url = _extract_code_link_from_arxiv_links(result.links)
        if repo_url:
            return repo_url

    repo_url = fetch_official_repo(paper_id)
    if _is_supported_repo_url(repo_url):
        return repo_url

    return None

def load_config(config_file:str) -> dict:
    '''
    config_file: input config file path
    return: a dict of configuration
    '''
    # make filters pretty
    def pretty_filters(**config) -> dict:
        keywords = dict()

        def parse_filters(filters: list) -> str:
            parsed = []
            for keyword in filters:
                term = f"\"{keyword}\"" if len(keyword.split()) > 1 else keyword
                parsed.append(term)
            return "(" + " OR ".join(parsed) + ")"

        for k, v in config["keywords"].items():
            keywords[k] = parse_filters(v["filters"])
        return keywords
    with open(config_file,'r') as f:
        config = yaml.load(f,Loader=yaml.FullLoader)
        config['kv'] = pretty_filters(**config)
        logging.info(
            "config = %s | arxiv.py version: %s", config, ARXIV_LIBRARY_VERSION
        )
    return config

def get_authors(authors, first_author = False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output
def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output    
def get_code_link(qword:str) -> str:
    """
    This short function was auto-generated by ChatGPT. 
    I only renamed some params and added some comments.
    @param qword: query string, eg. arxiv ids and paper titles
    @return paper_code in github: string, if not found, return None
    """
    # query = f"arxiv:{arxiv_id}"
    query = f"{qword}"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc"
    }
    r = requests.get(github_url, params=params, timeout=REQUEST_TIMEOUT)
    results = r.json()
    code_link = None
    if results["total_count"] > 0:
        code_link = results["items"][0]["html_url"]
    return code_link


def iter_search_results(search_engine):
    """Yield results from the arxiv client with basic error handling."""

    try:
        return ARXIV_CLIENT.results(search_engine)
    except Exception as exc:  # noqa: BLE001
        logging.error(
            "arXiv search failed for %s: %s", getattr(search_engine, "query", "<unknown>"), exc
        )
        return []

def get_daily_papers(topic,query="slam", max_results=2):
    """
    @param topic: str
    @param query: str
    @return paper_with_code: dict
    """
    # output 
    content = dict() 
    content_to_web = dict()
    search_engine = arxiv.Search(
        query = query,
        max_results = max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    for result in iter_search_results(search_engine):

        paper_id            = result.get_short_id()
        paper_title         = result.title
        paper_url           = result.entry_id
        paper_abstract      = result.summary.replace("\n"," ")
        paper_authors       = get_authors(result.authors)
        paper_first_author  = get_authors(result.authors,first_author = True)
        primary_category    = result.primary_category
        publish_time        = result.published.date()
        update_time         = result.updated.date()
        comments            = result.comment

        logging.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

        # eg: 2108.09112v1 -> 2108.09112
        ver_pos = paper_id.find('v')
        if ver_pos == -1:
            paper_key = paper_id
        else:
            paper_key = paper_id[0:ver_pos]    
        paper_url = arxiv_url + 'abs/' + paper_key
        
        repo_url = find_code_repository(paper_id, paper_title, result)

        if repo_url is not None:
            content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|**[link]({})**|\n".format(
                   update_time,paper_title,paper_first_author,paper_key,paper_url,repo_url)
            content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({}), Code: **[{}]({})**".format(
                   update_time,paper_title,paper_first_author,paper_url,paper_url,repo_url,repo_url)

        else:
            content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|null|\n".format(
                   update_time,paper_title,paper_first_author,paper_key,paper_url)
            content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({})".format(
                   update_time,paper_title,paper_first_author,paper_url,paper_url)

        # TODO: select useful comments
        comments = None
        if comments != None:
            content_to_web[paper_key] += f", {comments}\n"
        else:
            content_to_web[paper_key] += f"\n"

    data = {topic:content}
    data_web = {topic:content_to_web}
    return data,data_web 

def update_paper_links(filename):
    '''
    weekly update paper links in json file 
    '''
    def parse_arxiv_string(s):
        parts = s.split("|")
        date = parts[1].strip()
        title = re.sub(r"\*", "", parts[2]).strip()
        authors = parts[3].strip()
        arxiv_id = parts[4].strip()
        code = parts[5].strip()
        arxiv_id = re.sub(r'v\d+', '', arxiv_id)
        return date,title,authors,arxiv_id,code

    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
        json_data = m.copy() 

        for keywords,v in json_data.items():
            logging.info(f'keywords = {keywords}')
            for paper_id,contents in v.items():
                contents = str(contents)

                update_time, paper_title, paper_first_author, paper_url, code_url = parse_arxiv_string(contents)

                contents = "|{}|{}|{}|{}|{}|\n".format(update_time,paper_title,paper_first_author,paper_url,code_url)
                json_data[keywords][paper_id] = str(contents)
                logging.info(f'paper_id = {paper_id}, contents = {contents}')
                
                valid_link = False if '|null|' in contents else True
                if valid_link:
                    continue
                repo_url = find_code_repository(
                    paper_id, paper_title, _fetch_single_arxiv_result(paper_id)
                )
                if repo_url is not None:
                    new_cont = contents.replace('|null|',f'|**[link]({repo_url})**|')
                    logging.info(f'ID = {paper_id}, contents = {new_cont}')
                    json_data[keywords][paper_id] = str(new_cont)
        # dump to json file
        with open(filename,"w") as f:
            json.dump(json_data,f)

def update_json_file(filename,data_dict):
    '''
    daily update json file using data_dict
    '''
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
    json_data = m.copy() 
    
    # update papers in each keywords         
    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]

            if keyword in json_data.keys():
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    with open(filename,"w") as f:
        json.dump(json_data,f)
    
def json_to_md(
    filename,
    md_filename,
    task="",
    to_web=False,
    use_title=True,
    use_tc=True,
    show_badge=True,
    use_b2t=True,
    latest_n=None,
    archive_path=None,
):
    """
    @param filename: str
    @param md_filename: str
    @return None
    """
    def pretty_math(s: str) -> str:
        """Ensure inline math is spaced correctly in markdown strings."""
        match = re.search(r"\$(.+?)\$", s)
        if match is None:
            return s
        math_start, math_end = match.span()
        space_trail = space_leading = ''
        if math_start > 0 and s[math_start - 1] not in (' ', '*'):
            space_trail = ' '
        if math_end < len(s) and s[math_end] not in (' ', '*'):
            space_leading = ' '
        return (
            s[:math_start]
            + f"{space_trail}${match.group(1).strip()}${space_leading}"
            + s[math_end:]
        )
  
    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-','.')

    def keyword_anchor(keyword: str) -> str:
        return re.sub(r"[^a-z0-9\-]+", "", keyword.replace(" ", "-").lower())
    
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)

    if latest_n is not None:
        # ------- short README with latest papers grouped by keyword -------
        latest_by_keyword = {}
        for keyword in sorted(data.keys()):
            day_content = data[keyword]
            if not day_content:
                continue
            day_content = sort_papers(day_content)
            keyword_entries = []
            for _, line in day_content.items():
                if line is None:
                    continue
                parsed_line = pretty_math(line)
                m = re.search(r"\*\*(\d{4}-\d{2}-\d{2})\*\*", parsed_line)
                date = m.group(1) if m else "0000-00-00"
                keyword_entries.append((date, parsed_line))

            keyword_entries.sort(reverse=True, key=lambda x: x[0])
            if keyword_entries:
                latest_by_keyword[keyword] = keyword_entries[:latest_n]

        with open(md_filename, "w") as f:
            if show_badge:
                f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
                f.write(f"[![Forks][forks-shield]][forks-url]\n")
                f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
                f.write(f"[![Issues][issues-shield]][issues-url]\n\n")

            f.write("# CV ArXiv Daily\n")
            f.write("Automatically collected computer vision papers from arXiv.\n\n")
            f.write(f"> Updated on {DateNow}\n")
            f.write("> Usage instructions: [here](./docs/README.md#usage)\n\n")

            f.write("## Latest Papers\n")
            if latest_by_keyword:
                f.write("<details>\n")
                f.write("  <summary>Jump to Keyword</summary>\n")
                f.write("  <ol>\n")
                for keyword in latest_by_keyword.keys():
                    kw = keyword_anchor(keyword)
                    f.write(f"    <li><a href=#{kw}>{keyword}</a></li>\n")
                f.write("  </ol>\n")
                f.write("</details>\n\n")

                for keyword, entries in latest_by_keyword.items():
                    f.write(f"### {keyword}\n")
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|---|---|---|---|---|\n")
                    for _, line in entries:
                        f.write(line)
                    f.write("\n")
            f.write("See the [full archive](./docs/daily_archive.md) for more papers.\n\n")

            if show_badge:
                f.write((f"[contributors-shield]: https://img.shields.io/github/"
                         f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
                f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                         f"cv-arxiv-daily/graphs/contributors\n"))
                f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                         f"cv-arxiv-daily.svg?style=for-the-badge\n"))
                f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                         f"cv-arxiv-daily/network/members\n"))
                f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                         f"cv-arxiv-daily.svg?style=for-the-badge\n"))
                f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                         f"cv-arxiv-daily/stargazers\n"))
                f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                         f"cv-arxiv-daily.svg?style=for-the-badge\n"))
                f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                         f"cv-arxiv-daily/issues\n\n"))

        if archive_path:
            with open(archive_path, "w") as af:
                af.write("# Daily Paper Archive\n")
                af.write(f"> Updated on {DateNow}\n\n")

                af.write("<details>\n")
                af.write("  <summary>Table of Contents</summary>\n")
                af.write("  <ol>\n")
                for keyword in sorted(data.keys()):
                    day_content = data[keyword]
                    if not day_content:
                        continue
                    kw = keyword_anchor(keyword)
                    af.write(f"    <li><a href=#{kw}>{keyword}</a></li>\n")
                af.write("  </ol>\n")
                af.write("</details>\n\n")

                for keyword in sorted(data.keys()):
                    day_content = data[keyword]
                    if not day_content:
                        continue
                    af.write(f"## {keyword}\n\n")
                    af.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    af.write("|---|---|---|---|---|\n")
                    day_content = sort_papers(day_content)
                    for _, line in day_content.items():
                        if line is not None:
                            af.write(pretty_math(line))
                    af.write("\n")

        logging.info(f"{task} finished")
        return

    # ------- original full-table behaviour -------
    with open(md_filename, "w+") as f:
        pass

    with open(md_filename, "a+") as f:

        if (use_title == True) and (to_web == True):
            f.write("---\n" + "layout: default\n" + "---\n\n")

        if show_badge == True:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")

        if use_title == True:
            f.write("## Updated on " + DateNow + "\n")
        else:
            f.write("> Updated on " + DateNow + "\n")

        f.write("> Usage instructions: [here](./docs/README.md#usage)\n\n")

        if use_tc == True:
            f.write("<details>\n")
            f.write("  <summary>Table of Contents</summary>\n")
            f.write("  <ol>\n")
            for keyword in sorted(data.keys()):
                day_content = data[keyword]
                if not day_content:
                    continue
                kw = keyword_anchor(keyword)
                f.write(f"    <li><a href=#{kw}>{keyword}</a></li>\n")
            f.write("  </ol>\n")
            f.write("</details>\n\n")

        for keyword in sorted(data.keys()):
            day_content = data[keyword]
            if not day_content:
                continue
            f.write(f"## {keyword}\n\n")

            if use_title == True:
                if to_web == False:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n" + "|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

            day_content = sort_papers(day_content)

            for _, v in day_content.items():
                if v is not None:
                    f.write(pretty_math(v))

            f.write("\n")

            if use_b2t:
                top_info = f"#Updated on {DateNow}"
                top_info = top_info.replace(' ','-').replace('.','')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")

        if show_badge == True:
            f.write((f"[contributors-shield]: https://img.shields.io/github/"
                     f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/graphs/contributors\n"))
            f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/network/members\n"))
            f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/stargazers\n"))
            f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/issues\n\n"))

    logging.info(f"{task} finished")

def demo(**config):
    # TODO: use config
    data_collector = []
    data_collector_web= []
    
    keywords = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')
    if config['update_paper_links'] == False:
        logging.info(f"GET daily papers begin")
        for topic, keyword in keywords.items():
            logging.info(f"Keyword: {topic}")
            data, data_web = get_daily_papers(topic, query = keyword,
                                            max_results = max_results)
            data_collector.append(data)
            data_collector_web.append(data_web)
            print("\n")
        logging.info(f"GET daily papers end")

    # 1. update README.md file
    if publish_readme:
        json_file = config['json_readme_path']
        md_file   = config['md_readme_path']
        # update paper links
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            # update json data
            update_json_file(json_file,data_collector)
        # json data to markdown
        json_to_md(
            json_file,
            md_file,
            task="Update Readme",
            show_badge=show_badge,
            latest_n=20,
            archive_path="./docs/daily_archive.md",
        )

    # 2. update docs/index.md file (to gitpage)
    if publish_gitpage:
        json_file = config['json_gitpage_path']
        md_file   = config['md_gitpage_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            update_json_file(json_file,data_collector)
        json_to_md(json_file, md_file, task ='Update GitPage', \
            to_web = True, show_badge = show_badge, \
            use_tc=False, use_b2t=False)

    # 3. Update docs/wechat.md file
    if publish_wechat:
        json_file = config['json_wechat_path']
        md_file   = config['md_wechat_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            update_json_file(json_file, data_collector_web)
        json_to_md(json_file, md_file, task ='Update Wechat', \
            to_web=False, use_title= False, show_badge = show_badge)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path',type=str, default='config.yaml',
                            help='configuration file path')
    parser.add_argument('--update_paper_links', default=False,
                        action="store_true",help='whether to update paper links etc.')                        
    args = parser.parse_args()
    config = load_config(args.config_path)
    config = {**config, 'update_paper_links':args.update_paper_links}
    demo(**config)

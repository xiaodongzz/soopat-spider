# !coding=utf-8
import os
import xlrd
import xlwt
import datetime
import re
import time
import random
import sys

from tornado import httpclient, gen, ioloop, queues
from bs4 import BeautifulSoup

import pandas as pd

df = pd.DataFrame(columns=["公司名称", "申请日", "申请号", "申请公布号", "申请公布日", "授权公告号", \
                           "授权公告日", "专利申请人", "发明人", "专利名称", "技术领域", \
                           "专利关键技术点分析（保护了什么创新技术点）"])

sys.path.extend([os.path.abspath(os.path.join(os.path.realpath(__file__), os.pardir, os.pardir))])
# sys.path.extend(['/Users/Lena/Project/Python/Spider/PatentData'])
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'PatentData.settings')
# application = get_wsgi_application()

# from Patent.models import Company, Patent

__author__ = 'Woody Huang'
__version__ = '1.0.0'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
default_file_path = os.path.join(BASE_DIR, 'Patent', 'data', 'companies.xlsx')
base_url = 'http://www.soopat.com/Home/Result?SearchWord=%s&&FMZL=Y&SYXX=Y&WGZL=Y'
companies_pool = queues.Queue()

total_companies_num = 0
fetched_companies_num = 0
failed_companies_num = 0
company_list = []
df_row = 1

user_agents = [
    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50,'
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11',
    'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)', ]


def load_company_list(file_path, company_name_col_index=2, skip_rows=1):
    """ This function load companies information from the .xlsx file,
     and save them to the database
    """
    global company_list
    data = xlrd.open_workbook(file_path).sheets()[0]
    nrows = data.nrows
    new_company = 0
    for row_index, company_name in enumerate(data.col_values(company_name_col_index)[skip_rows:]):
        company_list.append(company_name)
        new_company += 1
    company_list = company_list.unique()
    print('{0}个公司的数据已经被存入数据库，其中新建的有{1}个'.format(nrows - 1, new_company))


@gen.coroutine
def parse_data_for_html(html_doc, company):
    """ This utility function parse useful data from the html text
    """
    soup = BeautifulSoup(html_doc, 'html.parser')
    # 首先检查是否遇到了验证码问题
    check = soup.find_all('img', {'src': '/Account/ValidateImage'})
    if check is not None and len(check) > 0:
        print('~~~~~~~~~~遇到验证码问题')
        raise gen.Return(-1)
    patent_blocks = soup.find_all('div', {'class': 'PatentBlock', 'style': None})
    for patent_block in patent_blocks:
        name = patent_block.find('input', {'name': 'cb'})['mc']
        patent_key = patent_block.h2.font.text
        patent_type = {u'[发明]': 'FM', u'[外观]': 'WG', u'[实用新型]': 'SY'
                       }[re.search(r'\[.*\]', patent_key).group()]
        note = patent_block.h2.a.find('font', {'size': -1}).text
        status = dict(
            stateicoinvalid='invalid',
            stateicovalid='valid',
            stateicopending='applying'
        )[patent_block.h2.div['class'][-1]]
        a_tag = patent_block.span.find_all('a')
        company_name = a_tag[0].text
        apply_at = re.search(r'\d\d\d\d-\d\d-\d\d', patent_block.span.text).group()
        category = a_tag[1].text
        abstract = patent_block.find('span', {'class': 'PatentContentBlock'}).text
        yield gen.Task(save_patent_to_database, dict(
            专利名称=name,
            申请时间=apply_at,
            摘要=abstract,
            类型=patent_type,
            状态=status,
            公司名称=company,
            分类=category,
            note=note
        ))
    raise gen.Return(len(patent_blocks))


def save_patent_to_database(data, callback):
    # first check whether the given pattern already exist
    if df["专利名称"].isin(data["专利名称"]):
        # 此处对表格进行标记，if是否有效
        print(u'专利%s已存在' % data['专利名称'])
        return callback()

    global df_row
    df.loc[df_row, 1] = data['公司名称'],
    df.loc[df_row, 2] = data['专利名称'],
    df.loc[df_row, 3] = datetime.datetime.strptime(data['申请时间'], '%Y-%m-%d'),
    df.loc[df_row, 4] = data['摘要'],
    df.loc[df_row, 5] = data['类型'],
    df.loc[df_row, 6] = data['状态'],
    df.loc[df_row, 7] = data['分类'],
    note = data['note']
    df_row = df_row + 1

    print(u'存入专利%s' % data['专利名称'])
    return callback()


@gen.coroutine
def search_for_company(company, skip=0):
    """ Search for the given company name, save the result to the database through ORM of django
     返回是一个Turple:
      (是否完成，完成的数量，未完成原因)
    """
    print(u'->开始搜索：%s' % company)
    fetched_patent = skip
    start_url = base_url % company.strip()
    # cookie = 'patentids=; domain=.soopat.com; expires=%s GMT; path=/' %\
    #          (timezone.now() + datetime.timedelta(seconds=60)).strftime('%a, %d-%b-%Y %H-%M-%S')
    cookie = 'lynx-randomcodestring=; patentids='
    client = httpclient.AsyncHTTPClient()
    while True:
        if fetched_patent > 0:
            request_url = start_url + '&PatentIndex=%s' % fetched_patent
        else:
            request_url = start_url
        print(u'开始发送访问请求：%s' % request_url)

        print('cookie::' + cookie)
        request = httpclient.HTTPRequest(url=request_url,
                                         headers={'Cookie': cookie,
                                                  'User-Agent': random.choice(user_agents)},
                                         follow_redirects=False, )
        response = yield client.fetch(request, raise_error=False)
        if response.code == 200:
            new_patents = yield parse_data_for_html(response.body, company)
            if 0 <= new_patents < 10:
                if new_patents == 0:
                    print(u'未能发现新的专利')
                break
            elif new_patents == -1:
                print(u'正在退出搜索: %s' % fetched_patent)
                # 如果遇到了验证码问题，返回进行休眠，通过返回告知上层目前进度
                raise gen.Return((False, fetched_patent, 'authenticate code'))
            fetched_patent += new_patents
            sleep_time = random.uniform(2, 10)
            print('正常工作间隔%s' % sleep_time)
            time.sleep(sleep_time)
            print(response.headers)
            cookie = response.headers.get('Set-Cookie', '')
        elif response.code == 500:
            print('遇到500错误，完成对当前条目的搜索')
            break
        else:
            print('出现其他返回状态代码：%s -> %s' % (response.code, response.headers.get('Location', '')))
            print(response.body)
            time.sleep(10)
            # 出现其他错误放弃
            client.close()
            raise gen.Return((False, 0, response.code))
    client.close()
    raise gen.Return((True, fetched_patent, None))


@gen.coroutine
def main():
    # 读取一些配置信息
    company_num = df["公司名称"].nunique()
    patent_num = df["专利名称"].len()
    if company_num > 0 or patent_num > 0:
        print("数据库中已有%s家公司的%s条专利数据" % (company_num, patent_num))
        print("\n\n")
        clear_old_data = input("是否清除已有的数据(y/[n])")
        if clear_old_data in ["y", "Y"]:
            Company.objects.all().delete()
            Patent.objects.all().delete()
            print("已清除原有数据!")
            print("\n\n\n\n\n")
    global total_companies_num
    # first make sure that all the company data are loaded
    print('###############爬虫启动！##################')
    print('从Excel文件中载入公司数据')
    default_path = os.path.join(BASE_DIR, 'data', 'companies.xlsx')
    path_option = input("默认输入文件路径是%s,是否使用其他输入文件(y/[n])" % default_path)
    if path_option in ["y", "Y"]:
        default_path = input("输入文件路径:").strip()
    load_company_list(default_path)
    print('载入完成')
    # Since the company number is not so large, load them into the queue
    print('将数据载入队列等待处理')

    for c in company_list:
        yield companies_pool.put(c)
        total_companies_num += 1

    print('载入完成，开始爬取数据，本次需要爬取的公司总数为: %s' % total_companies_num)

    @gen.coroutine
    def worker(worker_id):
        global fetched_companies_num
        global failed_companies_num
        global total_companies_num

        print('WORKER %s START!' % worker_id)
        finished = True
        skip = 0
        code_error_times = 0  # 连续发生验证码阻碍的次数
        while True:
            if finished:
                next_company = yield companies_pool.get()
                skip = df["公司名称" == next_company].count()
            finished, skip, reason = yield search_for_company(next_company, skip=skip)
            if not finished:
                if reason == 'authenticate code':
                    code_error_times += 1
                    sleep_time = min(random.uniform(10, 100) * code_error_times, random.uniform(400, 410))
                    print(u'】WORKER %s 进入休眠，本轮休眠时间为：%s' % (worker_id, sleep_time))
                    time.sleep(sleep_time)  # If fails, sleep a random time
                    print(u'】WORKER %s 恢复工作' % worker_id)
                else:
                    # 其他原因
                    code_error_times = 0
                    companies_pool.task_done()
                    failed_companies_num += 1
                    total_companies_num -= 1
                    finished = True
                    print(u"对【%s】的专利数据查询失败,目前失败总数为%s" % (next_company.name, failed_companies_num))
            else:
                code_error_times = 0
                fetched_companies_num += 1
                next_company.checked = True
                next_company.save()
                print(u'完成对【%s】的专利数据查询，目前进度%s/%s' % (
                    next_company.name, fetched_companies_num, total_companies_num))
                companies_pool.task_done()
                time.sleep(10)

    for i in range(1):
        worker(i)
    yield companies_pool.join()
    print('###############爬虫停止！##################')
    print('##########数据导出到output.xlsx############')
    write_database_to_excel()


def write_database_to_excel():
    book = xlwt.Workbook()
    sheet = book.add_sheet(u'专利数据', cell_overwrite_ok=True)
    companies = company_list

    global df
    df = df.drop_duplicates(subset="专利名称", keep="first")

    path = os.path.join(BASE_DIR, 'data', )
    folder = os.path.exists(path)
    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print
        "---  new folder...  ---"
        print
        "---  OK  ---"

    else:
        print
        "---  There is this folder!  ---"

    df.to_excel(os.path.join(BASE_DIR, 'data', '专利分析报告.xlsx'), mode='w')
    #        page_content = '\n'.join(page.extract_text().split('\n'))  # 处理读取到的字符串
    #        content = content+page_content

    print('finished')


if __name__ == '__main__':
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(main)

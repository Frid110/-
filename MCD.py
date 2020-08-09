import asyncio
from pyppeteer import launch
from fake_useragent import UserAgent
import json
import requests
import aiohttp
import sys
import re
import time 
import random
import sqlite3
import async_timeout
import aiosqlite
from lxml import etree
from aiostream import stream
from async_retrying import retry
from subprocess import call
import os
import sqlite3
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import *
import webbrowser
import threading



def search_exe(filename):
    chrome_path = []
    disks = (os.popen('fsutil fsinfo drives').read()).split()[1:]
    for path in disks:
        for root,dirs,files in os.walk(path):
            for name in files:
                if (name == filename):
                    chrome_path.append(os.path.join(root,name))
                    return chrome_path[0]


async def open_browser():
    #设置浏览器参数并打开页面
    ua = UserAgent().random
    url = r'https://www.metmuseum.org/art/collection/search'
    w, h = 1344,840
    print('正在搜索本地谷歌浏览器位置')
    executablePath = search_exe('chrome.exe')#r'.\Chrome\App\Google Chrome\chrome.exe'
    if executablePath:
        print('已找到您的浏览器,启动中......')
    else:
        print('请先安装谷歌浏览器，或下载附带浏览器版本')
    browser = await launch(headless=True, 
                            executablePath=executablePath,
                            dumpio=False,
                            options={'args':
                            ['--no-sandbox',
                            '--disable-infobars',
                            f'--window-size={w},{h}',
                            '--disable-extensions',
                            '--hide-scrollbars',
                            '--disable-bundled-ppapi-flash',
                            '--mute-audio',
                            '--disable-setuid-sandbox',
                            '--disable-gpu'
                            ]},
                            )
    page = await browser.newPage()
    await page.setUserAgent(ua)
    await page.setJavaScriptEnabled(enabled=False)#启用页面渲染
    await page.setViewport({"width": w,"height": h})#显示页面大小
    await page.goto(url)#,options={'timeout':'3*30000'}
    cookies_list = await page.cookies()
    cookies = ''
    for cookie in cookies_list:
        str_cookie = '{0}={1};'
        str_cookie = str_cookie.format(cookie.get('name'), cookie.get('value'))
        cookies += str_cookie
        
    await browser.close()
    return cookies
    
def get_cookies():
    return asyncio.get_event_loop().run_until_complete(open_browser())

######################################################################################

class MSqlite(object):
    '''表格操作'''
    def creat_urlall(self):
        command = '''CREATE TABLE sheet(id INTEGER PRIMARY KEY AUTOINCREMENT,
            statue_code int,
            原图检索号 int,
            普通图片检索号 int,
            音频检索号 int,
            title text,
            detail_link  text,
            weblarge_img text,
            accessionNumber text,
            artist  text,
            time  text,
            description  text,
            originalimg_link  text,
            medium  text,
            Series_Portfolio  text,
            Dimensions  text,
            Credit_Line  text,
            audio text,
            album text
                );'''
        if os.path.exists('下载目录\\{}_allurls.db'.format(query)):
            os.remove('下载目录\\{}_allurls.db'.format(query))
        self.conn = sqlite3.connect('下载目录\\{}_allurls.db'.format(query))
        self.c = self.conn.cursor()
        self.c.execute(command)
        self.conn.commit()
        self.conn.close()
        
    #存储url
    def save_urlall(self,statue_code,imgweb_index,detail_url,weblarge_img,accessionNumber):
        self.conn = sqlite3.connect('下载目录\\{}_allurls.db'.format(query))
        self.c = self.conn.cursor()
        self.c.execute("INSERT into sheet(statue_code,普通图片检索号,detail_link,weblarge_img,accessionNumber)VALUES('{0}','{1}','{2}','{3}','{4}')".format(statue_code,imgweb_index,detail_url,weblarge_img,accessionNumber))
        self.conn.commit()
        self.conn.close()


db = MSqlite()
def get_massage(text):
    infos = json.loads(text)['results']
    #循环提取
    for info in infos:
        # title = str(info['title']).replace(r'[\/:*?"<>|,]','_').replace('\n','')
        if str(info['image']).find('NoImageAvailableIcon') == -1:
            weblarge_img = str(info['image']).replace('mobile-large','web-large')
        else:
            weblarge_img = '无'
        detail_url = str(info['url']).split('?')[0]
        accessionNumber = str(info['accessionNumber'])
        statue_code = 0
        imgweb_index = weblarge_img.split('/')[-1] or '无'
        db.save_urlall(statue_code,imgweb_index,detail_url,weblarge_img,accessionNumber)
    time.sleep(random.randint(0,4))
    


def get_urllists(material,searchField,era,geolocation,department,showOnly):
    db.creat_urlall()
    params = {
        'artist': '',
        'department': department,
        'era': '',
        'geolocation': geolocation,
        'material': material,
        'offset': '0',
        'pageSize': '0',
        'perPage': '80',
        'q': query,  #搜索关键词
        'searchField': searchField,
        'showOnly': showOnly,
        'sortBy': 'Relevance',
        'sortOrder': 'asc'
        }

    url_search = 'https://www.metmuseum.org/mothra/collectionlisting/search'
    response1 = requests.get(url_search, headers=headers,params=params)
    # print(response1.text)
    totalResults = json.loads(response1.text)['totalResults']
    totalPage = int(totalResults/80)+1
    print('共检索出%s个匹配项,总计%s页'%(totalResults,totalPage))

    #循环翻页
    n = 0
    while True:
        params['offset'] = n*80
        response2 = requests.get(url_search, headers=headers,params=params)
        # a = re.search('META NAME="ROBOTS"',response2.text,re.IGNORECASE)
        # if (a is None):
        if response2.text[:100].upper().find('META NAME="ROBOTS"') == -1:
            print('正在爬取第%d页'%(n+1))
            get_massage(response2.text)
            
        else:
            headers['cookie'] = get_cookies()
            print('被检测到是机器人，已更新cookie，重新爬取中......')
            try:
                get_massage(response2.text)
                
            except:
                print('本页未能爬取，请联系开发者解决故障，WX:manguo01')
                time.sleep(random.randint(0,4))
        n += 1
        if totalPage <= n:
            print('已成功爬取所有详情页链接')
            print('准备下载详情页内容，请耐心等待')
            break

##########################################################################################

async def db_update(statue_code,title,orgimg_index,audio_index,artist,time,description,originalimg_link,medium,Series_Portfolio,Dimensions,Credit_Line,audio,album,accessionNumber):
    async with aiosqlite.connect('下载目录\\{}_allurls.db'.format(query),check_same_thread=False) as db:
        sql = 'UPDATE sheet SET statue_code=?,原图检索号=?,音频检索号=?,title=?,artist=?,time=?,description=?,originalimg_link=?,medium=?,Series_Portfolio=?,Dimensions=?,Credit_Line=?,audio=?,album=? WHERE accessionNumber=? '
        await db.execute(sql,(statue_code,title,orgimg_index,audio_index,artist,time,description,originalimg_link,medium,Series_Portfolio,Dimensions,Credit_Line,audio,album,accessionNumber))
        await db.commit()


def generator():
    conn = sqlite3.connect('下载目录\\{}_allurls.db'.format(query))
    sql = "SELECT detail_link from sheet where statue_code=?"
    c = conn.cursor()
    c.execute(sql, (0,))
    data = c.fetchall()
    for url in data:
        yield url[0]
    conn.close()




async def save_content(text):
    html = etree.HTML(text)
    title = str((html.xpath('//span[@class="artwork__title--text"]/text()') or '无')[0])
    description = str((html.xpath('//div[@itemprop="description"]/p/text()') or '无')[0])
    tm = str((html.xpath('//time[@itemprop="dateCreated"]/text()') or '无')[0])
    t = re.sub('\n ','',tm).strip()
    originalimg_link = (html.xpath('//li[@class="artwork__interaction artwork__interaction--download"]/a/@href') or '无')[0]
    artist = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Artist:"]/following-sibling::span[1]/text()') or '无'))
    medium = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Medium:"]/following-sibling::span[1]/text()') or '无'))
    Series_Portfolio = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Series/Portfolio:"]/following-sibling::span[1]/text()') or '无'))
    Dimensions = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Dimensions:"]/following-sibling::span[1]/text()') or '无'))
    Credit_Line  = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Credit Line:"]/following-sibling::span[1]/text()') or '无'))
    Accession_Number = (''.join(html.xpath('//p[@class="artwork__tombstone--row"]/span[text()="Accession Number:"]/following-sibling::span[1]/text()') or '无'))
    # inscription = (''.join(html.xpath('//div[@class="accordion__content"]/text()') or '无'))
    audios = html.xpath('//div[@class="artwork-audio-item__player met-audio"]/audio/source/@src') or '无'
    audio = ('|'.join(audios))#提取时按列表处理
    albums = html.xpath('//div[@class="met-carousel__item"]/img/@data-superjumboimage') or '无'
    album = ('|'.join(albums))
    orgimg_index = (originalimg_link.split('/')[-1] or '无') + ';' + ';'.join(re.findall("(?<=original/).*?(?=')",str(albums)))
    # print(orgimg_index)
    audio_index = ';'.join(re.findall("(?<=audio/).*?(?=')",str(audios)))
    # print(audio_index)
    print('正在下载：'+title)
    title = re.sub("'",'’',title)
    artist = re.sub("'",'’',artist) 
    t = re.sub("'",'’',t) 
    description = re.sub("'",'’',description) 
    originalimg_link = re.sub("'",'’',originalimg_link) 
    medium = re.sub("'",'’',medium) 
    Series_Portfolio = re.sub("'",'’',Series_Portfolio) 
    Dimensions = re.sub("'",'’',Dimensions) 
    Credit_Line = re.sub("'",'’',Credit_Line) 
    accessionNumber = re.sub("'",'’',Accession_Number)
    # inscription = re.sub("'",'’',inscription)
    statue_code = 1
    
    await db_update(statue_code,orgimg_index,audio_index,title,artist,t,description,originalimg_link,medium,Series_Portfolio,Dimensions,Credit_Line,audio,album,accessionNumber)

    # await asyncio.sleep(random.randint(0,4))

@retry(attempts = 3)
async def fetch(session, url,headers):
    # with async_timeout.timeout(60):
        # async with semaphore:
    async with session.get(url) as response:
        text = await response.text()
        # a = re.search('META NAME="ROBOTS"',text,re.IGNORECASE)
        if text[:100].upper().find('META NAME="ROBOTS"') == -1:
            await save_content(text)
        else:
            print(text[:100])
            headers['cookie'] = get_cookies()
            print('被检测到是机器人，已更新cookie，正在重新爬取......')
            try:
                r = requests.get(detail_url,headers=headers)
                text = await r.text()
                await save_content(text)
            except:
                print('本页未能爬取，请联系开发者解决故障，WX:manguo01')    
            await asyncio.sleep(random.randint(1,4))


async def branch(item):
    index = 0
    limit = 5

    while True:
       #测试发现item应该是个callable类型，并且运行之后是个async_generator。
        xs = stream.iterate(item)
        ys = xs[index:index + limit]
        each_tasks = await stream.list(ys)
        if not each_tasks:
            break

        await asyncio.ensure_future(asyncio.gather(*each_tasks))
        # await asyncio.sleep(3)
        index += limit


async def main(headers):
    # semaphore = asyncio.Semaphore(15)
    #提取链接
    async with aiosqlite.connect('下载目录\\{}_allurls.db'.format(query),check_same_thread=False) as conn:
        sql = "SELECT detail_link from sheet where statue_code=?"
        cursor = await conn.execute(sql, (0,))
        rows = await cursor.fetchall()
    async with aiohttp.connector.TCPConnector(limit=300, force_close=True, enable_cleanup_closed=True,ssl=False) as tc:
        async with aiohttp.ClientSession(connector=tc,headers=headers) as session:
            #包装成异步任务
            coros = (asyncio.ensure_future(fetch(session, url[0],headers)) for url in rows)
            # await asyncio.ensure_future(asyncio.gather(*coros))
            return await branch(coros)


def runall():
    now = lambda: time.time()
    starttime = now()
    while True:
        print(asyncio.run(main(headers)))
        try:
            res = next(generator())
            print(res)
        except Exception as ret:
            print(ret)
            print('文件下载完成，请前往检查')
            break
    print('共耗时：%d秒'%(now() - starttime))

####################################################################################


IDM = r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe"  #IDM程序在电脑上的位置
if not os.path.exists(r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe"):
    IDM = r"C:\Program Files\Internet Download Manager\IDMan.exe"
    if not os.path.exists( r"C:\Program Files\Internet Download Manager\IDMan.exe"):
        IDM = search_exe('IDMan.exe')
        if not os.path.exists(IDM):
            print('下载图片请先前往安装包安装IDM下载器')
#original/weblarge
def select_item(query,link_type):
    conn = sqlite3.connect('下载目录\\{}_allurls.db'.format(query))
    sql = "SELECT {} from sheet".format(link_type)
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    for url in rows:
        yield url[0]
    conn.close()
#audio/album    
def select_items(query,link_type):
    conn = sqlite3.connect('下载目录\\{}_allurls.db'.format(query))
    sql = "SELECT {} from sheet".format(link_type)
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    for urls in rows:
        for url in urls[0].split('|'):
            yield url
    conn.close()

def download_originalimg(query):
    thisPath=os.getcwd()
    DownPath = thisPath + r'\下载目录\original_img_{}'.format(query) 
    if not os.path.exists(DownPath):
        os.makedirs(DownPath)  
    #将下载链接全部加入到下载列表，之后再进行下载。
    for url in select_item(query,'originalimg_link'):
        print(url)    
        call([IDM, '/d',url,'/p',DownPath,'/n','/a'])
    call([IDM,'/s'])

def download_albumimg(query):
    thisPath=os.getcwd()
    DownPath = thisPath + r'\下载目录\original_img_{}'.format(query)  
    if not os.path.exists(DownPath):
        os.makedirs(DownPath)  
    #将下载链接全部加入到下载列表，之后再进行下载。
    for url in select_items(query,'album'):
        print(url)    
        call([IDM, '/d',url,'/p',DownPath,'/n','/a'])
    call([IDM,'/s'])

def download_weblargeimg(query):
    thisPath=os.getcwd()
    DownPath = thisPath + r'\下载目录\weblarge_img_{}'.format(query)   
    if not os.path.exists(DownPath):
        os.makedirs(DownPath)  
    #将下载链接全部加入到下载列表，之后再进行下载。
    for url in select_item(query,'weblarge_img'):
        print(url)    
        call([IDM, '/d',url,'/p',DownPath,'/n','/a'])
    call([IDM,'/s'])

def download_audio(query):
    thisPath=os.getcwd()
    DownPath = thisPath + r'\下载目录\audio_{}'.format(query)   
    if not os.path.exists(DownPath):
        os.makedirs(DownPath)  
    #将下载链接全部加入到下载列表，之后再进行下载。
    for url in select_items(query,'audio'):
        print(url)    
        call([IDM, '/d',url,'/p',DownPath,'/n','/a'])
    call([IDM,'/s'])
########################################################################

class win:
    def __init__(self, root=None):

        root.geometry("738x499")
        root.minsize(152, 1)
        root.maxsize(1684, 1025)
        root.resizable(1, 1)
        root.title("MMoA")

        self.icon = PhotoImage(file='icon.png')
        self.canvers = Canvas(root,width=70,height=70)
        self.canvers.place(relx=0.85, rely=0.09)
        self.canvers.create_image(1,1,anchor=NW,image=self.icon)

        # self.Label0 = tk.Label(root)
        # self.Label0.place(relx=0.068, rely=0.26, height=62, width=118)
        # self.Label0.configure(text='''按艺术家搜索''')

        # self.Entry0 = tk.Entry(root)
        # self.Entry0.place(relx=0.217, rely=0.28,height=41, relwidth=0.466)

        self.Label1 = tk.Label(root)
        self.Label1.place(relx=0.068, rely=0.08, height=62, width=116)
        self.Label1.configure(text='''搜索词''')

        self.Entry1 = tk.Entry(root)
        self.Entry1.place(relx=0.217, rely=0.1,height=41, relwidth=0.6)

        self.menubar = tk.Menu(root,font="TkMenuFont")
        root.configure(menu = self.menubar)

        self.Label2 = tk.Label(root)
        self.Label2.place(relx=0.122, rely=0.20, height=26, width=45)
        self.Label2.configure(text='''Field''')

        self.Combobox1 = ttk.Combobox(root,value=('All','ArtistCulture','Title','Description','Gallery','AccessionNum'))
        self.Combobox1.place(relx=0.217, rely=0.20, relheight=0.052, relwidth=0.198)
        self.Combobox1.current(0)

        self.Label3 = tk.Label(root)
        self.Label3.place(relx=0.095, rely=0.28, height=26, width=85)
        self.Label3.configure(text='''Filter By:''')
        
        self.Label4 = tk.Label(root)
        self.Label4.place(relx=0.217, rely=0.28, height=26, width=115)
        self.Label4.configure(text='''Material''')

        self.Label4_5 = tk.Label(root)
        self.Label4_5.place(relx=0.42, rely=0.28, height=26, width=94)
        self.Label4_5.configure(text='''Location''')

        self.Label5 = tk.Label(root)
        self.Label5.place(relx=0.61, rely=0.28, height=26, width=95)
        self.Label5.configure(text='''Date/Era''')

        self.Label5_6 = tk.Label(root)
        self.Label5_6.place(relx=0.799, rely=0.28, height=26, width=95)
        self.Label5_6.configure(text='''Department''')

        self.Entry2 = tk.Entry(root)
        self.Entry2.place(relx=0.217, rely=0.36,height=29, relwidth=0.154)
        self.Entry2.insert(END,'Paintings')

        self.Entry3 = tk.Entry(root)
        self.Entry3.place(relx=0.407, rely=0.36,height=29, relwidth=0.154)

        self.Entry4 = tk.Entry(root)
        self.Entry4.place(relx=0.596, rely=0.36,height=29, relwidth=0.154)

        self.Entry5 = tk.Entry(root)
        self.Entry5.place(relx=0.786, rely=0.36,height=29, relwidth=0.154)

        self.Label6 = tk.Label(root)
        self.Label6.place(relx=0.44, rely=0.19, height=38, width=75)
        self.Label6.configure(text='''_________\n{筛选条件}\n''', font = r"-family {宋体} -size 10 -weight bold -underline 1")

        self.Label7 = tk.Label(root)
        self.Label7.place(relx=0.217, rely=0.44, height=26, width=535)
        self.Label7.configure(text='''以上务必按照官网提供的字段填写，多个字段并选请用“|”号隔开（默认全选）''')

        self.menubar = tk.Menu(root,font="TkMenuFont")
        root.configure(menu = self.menubar)
#######################    Checkbutton    ###############################
        self.Label3 = tk.Label(root)
        self.Label3.place(relx=0.095, rely=0.50)
        self.Label3.configure(text='''Show Only:''')

        self.check1 = tk.BooleanVar()
        self.checkbox1 = tk.Checkbutton(root,text="Highlights",variable=self.check1)
        self.checkbox1.place(relx=0.216, rely=0.50 )

        self.check2 = tk.BooleanVar()
        self.checkbox2 = tk.Checkbutton(root,text="Artworks With Image",variable=self.check2)
        self.checkbox2.place(relx=0.371, rely=0.50 )

        self.check3 = tk.BooleanVar()
        self.checkbox3 = tk.Checkbutton(root,text="Artworks On Display",variable=self.check3)
        self.checkbox3.place(relx=0.583, rely=0.50 )

        self.check4 = tk.BooleanVar()
        self.checkbox4 = tk.Checkbutton(root,text="Open Access",variable=self.check4)
        self.checkbox4.place(relx=0.805, rely=0.50)
#########################    button    ################################
        self.Label0 = tk.Label(root)
        self.Label0.place(relx=0.1, rely=0.6, height=100, width=92)
        self.Label0.configure(background="#d9d9d9")
        self.Label0.configure(text='''为获得\n更好的下载体\n验，请先安装\nIDM下载器''')
        
        self.Button1 = tk.Button(root)
        self.Button1.place(relx=0.25, rely=0.65, height=53, width=163)
        self.Button1.configure(command=self.out)
        self.Button1.configure(text='''①抓取链接并下载详情页''')

        self.Button2 = tk.Button(root)
        self.Button2.place(relx=0.5, rely=0.65, height=53, width=163)
        self.Button2.configure(command=self.download_orgimg)
        self.Button2.configure(text='''②下载原图''')

        self.Button2 = tk.Button(root)
        self.Button2.place(relx=0.74, rely=0.63, height=30, width=79)
        self.Button2.configure(command=self.download_audio)
        self.Button2.configure(text='''下载解说音频''')

        self.Button2 = tk.Button(root)
        self.Button2.place(relx=0.74, rely=0.7, height=30, width=79)
        self.Button2.configure(command=self.download_weblargeimg)
        self.Button2.configure(text='''下载普通大图''')
        

        # self.TProgressbar1 = ttk.Progressbar(root)
        # self.TProgressbar1.place(relx=0.029, rely=0.922, relwidth=0.948, relheight=0.0, height=22)
        # self.TProgressbar1.configure(length="660")

    def __out(self):
        # sortBy = 'Date'
        # sortOrder = 'asc'
        # artist = self.Entry0.get()
        print('爬虫机器人开始工作，爬取过程中请勿重复点击按钮')
        global query
        query = self.Entry1.get()
        searchField = self.Combobox1.get()
        material = self.Entry2.get()
        geolocation = self.Entry3.get()
        era = self.Entry4.get()
        department = self.Entry5.get()
        showonly = []
        if self.check1.get() == True:
            showonly.append('highlights')
        if self.check2.get() == True:
            showonly.append('withImage')
        if self.check3.get() == True:
            showonly.append('onDisplay')
        if self.check4.get() == True:
            showonly.append('openAccess')
        showonly = '|'.join(showonly)
        get_urllists(material,searchField,era,geolocation,department,showonly)
        runall()

    def out(self):
        thrd = threading.Thread(target=self.__out)
        thrd.setDaemon(True)
        thrd.start()

    def __download_orgimg(self):
        query = self.Entry1.get()
        download_originalimg(query)
        download_albumimg(query)

    def download_orgimg(self):
        thrd = threading.Thread(target=self.__download_orgimg)
        thrd.setDaemon(True)
        thrd.start()

    def __download_audio(self):
        query = self.Entry1.get()
        download_audio(query)

    def download_audio(self):
        thrd = threading.Thread(target=self.__download_audio)
        thrd.setDaemon(True)
        thrd.start()

    def __download_weblargeimg(self):
        query = self.Entry1.get()
        download_weblargeimg(query)

    def download_weblargeimg(self):
        thrd = threading.Thread(target=self.__download_weblargeimg)
        thrd.setDaemon(True)
        thrd.start()


if __name__ == "__main__":
    webbrowser.open('https://www.metmuseum.org/art/collection')
    headers = {
        'user-agent': UserAgent().random,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36 Edg/80.0.361.109',
        'cookie': get_cookies()
    }
    root = Tk()
    app = win(root)
    root.mainloop()
    




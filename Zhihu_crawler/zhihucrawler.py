import requests
import json
import pymysql
from bs4 import BeautifulSoup as BS
import logging
import time
import re

fmt = '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S'
level = logging.INFO

formatter = logging.Formatter(fmt, datefmt)
logger = logging.getLogger()
logger.setLevel(level)

file = logging.FileHandler("../zhihu.log", encoding='utf-8')
file.setLevel(level)
file.setFormatter(formatter)
logger.addHandler(file)

console = logging.StreamHandler()
console.setLevel(level)
console.setFormatter(formatter)
logger.addHandler(console)


class ZhihuCrawler:
    def __init__(self):
        with open("zhihu.json", "r", encoding="utf8") as f:
            self.settings = json.load(f)  # Load settings
        logger.info("Settings loaded")


    def sleep(self, sleep_key, delta=0):
        """
        Execute sleeping for a time configured in the settings
        :param sleep_key: the sleep time label
        :param delta: added to the sleep time
        :return:
        """
        _t = self.settings["config"][sleep_key] + delta
        logger.info(f"Sleep {_t} second(s)")
        time.sleep(_t)

    def query(self, sql, args=None, op=None):
        """
        Execute an SQL query
        :param sql: the SQL query to execute
        :param args: the arguments in the query
        :param op: the operation to cursor after query
        :return: op(cur)
        """
        conn = pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            **self.settings['mysql']
        )
        if args and not (isinstance(args, tuple) or isinstance(args, list)):
            args = (args,)
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, args)
                    conn.commit()
                    if op is not None:
                        return op(cur)
                except:  # Log query then exit
                    if hasattr(cur, "_last_executed"):
                        logger.error("Exception @ " + cur._last_executed)
                    else:
                        logger.error("Exception @ " + sql)
                    raise

    def watch(self, top=None):
        """
        The crawling flow
        :param top: only look at the first `top` entries in the board. It can be used when debugging
        :return:
        """
        self.create_table()
        while True:
            logger.info("Begin crawling ...")
            try:
                crawl_id = None
                begin_time = time.time()
                crawl_id = self.begin_crawl(begin_time)

                try:
                    board_entries = self.get_board()
                except RuntimeError as e:
                    if isinstance(e.args[0], requests.Response):
                        logger.exception(e.args[0].status_code, e.args[0].text)
                    raise
                else:
                    logger.info(
                        f"Get {len(board_entries)} items: {','.join(map(lambda x: x['title'][:20], board_entries))}")
                if top:
                    board_entries = board_entries[:top]

                # Process each entry in the hot list
                for idx, item in enumerate(board_entries):
                    self.sleep("interval_between_question")
                    detail = {
                        "created": None,
                        "visitCount": None,
                        "followerCount": None,
                        "answerCount": None,
                        "raw": None,
                        "hit_at": None
                    }
                    if item["qid"] is None:
                        logger.warning(f"Unparsed URL @ {item['url']} ranking {idx} in crawl {crawl_id}.")
                    else:
                        try:
                            detail = self.get_question(item["qid"])
                        except Exception as e:
                            if len(e.args) > 0 and isinstance(e.args[0], requests.Response):
                                logger.exception(f"{e}; {e.args[0].status_code}; {e.args[0].text}")
                            else:
                                logger.exception(f"{str(e)}")
                        else:
                            logger.info(f"Get question detail for {item['title']}: raw detail length {len(detail['raw']) if detail['raw'] else 0}")
                    try:
                        self.add_entry(crawl_id, idx, item, detail)
                    except Exception as e:
                        logger.exception(f"Exception when adding entry {e}")
                self.end_crawl(crawl_id)
            except Exception as e:
                logger.exception(f"Crawl {crawl_id} encountered an exception {e}. This crawl stopped.")
            self.sleep("interval_between_board", delta=(begin_time - time.time()))

    def create_table(self):
        """
        Create tables to store the hot question records and crawl records
        """
        sql = f"""
CREATE TABLE IF NOT EXISTS `crawl` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `begin` DOUBLE NOT NULL,
    `end` DOUBLE,
    PRIMARY KEY (`id`) USING BTREE
)
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;
CREATE TABLE IF NOT EXISTS `record`  (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `qid` INT NOT NULL,
    `crawl_id` BIGINT NOT NULL,
    `hit_at` DOUBLE,
    `ranking` INT NOT NULL,
    `title` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL ,
    `heat` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `created` INT,
    `visitCount` INT,
    `followerCount` INT,
    `answerCount` INT,
    `excerpt` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    `raw` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ,
    `url` VARCHAR(255),
    PRIMARY KEY (`id`) USING BTREE,
    INDEX `CrawlAssociation` (`crawl_id`) USING BTREE,
    CONSTRAINT `CrawlAssociationFK` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
) 
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;
"""
        self.query(sql)

    def begin_crawl(self, begin_time) -> (int, float):
        """
        Mark the beginning of a crawl
        :param begin_time:
        :return: (Crawl ID, the time marked when crawl begin)
        """
        sql = """
INSERT INTO crawl (begin) VALUES(%s);
"""
        return self.query(sql, begin_time, lambda x: x.lastrowid)

    def end_crawl(self, crawl_id: int):
        """
        Mark the ending time of a crawl
        :param crawl_id: Crawl ID
        """
        sql = """
UPDATE crawl SET end = %s WHERE id = %s;
"""
        self.query(sql, (time.time(), crawl_id))

    def add_entry(self, crawl_id, idx, board, detail):
        """
        Add a question entry to database
        :param crawl_id: Crawl ID
        :param idx: Ranking in the board
        :param board: dict, info from the board
        :param detail: dict, info from the detail page
        """
        sql = \
            """
INSERT INTO record (`qid`, `crawl_id`, `title`, `heat`, `created`, `visitCount`, `followerCount`, `answerCount`,`excerpt`, `raw`, `ranking`, `hit_at`, `url`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
        self.query(
            sql,
            (
                board["qid"],
                crawl_id,
                board["title"],
                board["heat"],
                detail["created"],
                detail["visitCount"],
                detail["followerCount"],
                detail["answerCount"],
                board["excerpt"],
                detail["raw"],
                idx,
                detail["hit_at"],
                board["url"]
            )
        )

    def get_board(self) -> list:
        """
        TODO: Fetch current hot questions
        :return: hot question list, ranking from high to low
        Return Example:
        [
            {
                'title': '针对近期生猪市场非理性行为，国家发展改革委研究投放猪肉储备，此举对市场将产生哪些积极影响？',
                'heat': '76万热度',
                'excerpt': '据国家发展改革委微信公众号 7 月 5 日消息，针对近期生猪市场出现盲目压栏惜售等非理性行为，国家发展改革委价格司正研究启动投放中央猪肉储备，并指导地方适时联动投放储备，形成调控合力，防范生猪价格过快上涨。',
                'url': 'https://www.zhihu.com/question/541600869',
                'qid': 541600869,
            },
            {
                'title': '有哪些描写夏天的古诗词？',
                'heat': '41万热度',
                'excerpt': None,
                'url': 'https://www.zhihu.com/question/541032225',
                'qid': 541032225,
            },
            {
                'title':    # 问题标题
                'heat':     # 问题热度
                'excerpt':  # 问题摘要
                'url':      # 问题网址
                'qid':      # 问题编号
            }
            ...
        ]
        """
        resp=requests.get("https://www.zhihu.com/billboard",headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
            #"Cookie": "_zap=31894feb-28a8-4f4f-add0-a9b6235e1f69; d_c0=\"AJBfkc6CDBSPThKM7LqcTvOMRl0lqlId3Fk=|1637216980\"; _9755xjdesxxd_=32; YD00517437729195%3AWM_TID=7zmQ3%2BSo%2B2RBURABRRZ%2Fo%2FUdhph4ezVE; _xsrf=3lX64M4nJJEwIZiIQ5lvaHu0oLgLjBhi; __snaker__id=fMQWxv7yCtqR35Qb; q_c1=15b9840395d64e4db7672b6de2bcbce5|1645945047000|1645945047000; __utma=155987696.1706522720.1652132224.1652132224.1652132224.1; __utmz=155987696.1652132224.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); gdxidpyhxdE=YOkTlWaV%5CojSNz55yq9tBEAswa2uhZEfRuNSju1spoA%2FrkkGXdj%2FQ%5Cfe%2BvdBrnlbf8QXhYNJnG5lq1ZHEcixs3Pf7xWB5EmBZN8N4TQzoPO8DaJbRbGZzXSBbXSfTxuhEJAkQWa3eJgHG0x9pEO31mh4Sr9sbO7xiS07U7ojmmcf3UXa%3A1656168682655; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6ee86b5739889a78fd6428cac8ea6d85a828a8fb0c54af8bf84abe273ab97a58ad42af0fea7c3b92a91babfa3f933a6ea9b86c9408392a794b75db3a6bcd0e247abbdbfa5e44fab9cfab2f15bb2e9a093e762f8adbba5c1349cb39ba6b87db6ecfdd5c959f6afad98c579aab08ed7e66282edff86c73fb389b8cce846e9beb7d7d573aa93ac82f76097a6b7ccca60a1efc083e94df1b7a1d9dc4db5abb88bcd3df8b8a2afc85f928bacd3c837e2a3; YD00517437729195%3AWM_NI=SfqBz9f6UeJ4ANis4Wqzt%2FcMicqDaEiD6QrQr7Prdj4suDB8XmUE5wv434JS6qcwFsjpB6gjB31eD5qWx%2BeN%2Bby6VyjMdatTUzy%2BKTc1pbjG3io8YGwuxjN74hnNEURUd2E%3D; captcha_session_v2=2|1:0|10:1656167782|18:captcha_session_v2|88:ZTJvUVRkOEh3VThEdEFyb2w5QnRoc3creHhuMll6T1VmSThJU0wxZFV4V2g4dDV3REZEY1ovWG9jS2FVU1NKVQ==|5a774b2d94c9855378c0074941035fbdba0870fa2147756d0602c4ca0bafb323; z_c0=2|1:0|10:1656167788|4:z_c0|92:Mi4xNlBBNkd3QUFBQUFBa0YtUnpvSU1GQmNBQUFCZ0FsVk5iR3VrWXdCVldmc2xiOVhXbXpZbXBTR3hKdDJNYnVocV9R|b4dc0257c397684b1de3136c4ff15b651e61fea0e095211bd6459fe23a1afdc5; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1657502029,1657810669,1657853584,1657864941; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1657864941; tst=h; NOT_UNREGISTER_WAITING=1; KLBRSID=af132c66e9ed2b57686ff5c489976b91|1657864941|1657864939; SESSIONID=BVSFMNdyDBkPAt1PaKoHf6Iw8INTDF7SZr3POnrAZz9; JOID=U1oVAEosdW49q-XHNi5WPxq7nNEjbSE-eeaooUxoNAdu6JH1ZMB2vl2o5MY3-160LArHhFTybnSmPv9DAaqk0SM=; osd=U1sQAUIsdGs8o-XGMy9ePxu-ndkjbCQ_ceappE1gNAZr6Zn1ZcV3tl2p4cc_-1-xLQLHhVHzZnSnO_5LAauh0Cs="
        })
        res_list=[]
        soup=BS(resp.text,'lxml')
        script_text=soup.find("script",id="js-initialData").get_text()
        result=re.findall(r'"hotList":(.*),"guestFeeds"',script_text)
        temp=result[0].replace("false","False").replace("true","True")
        hot_list=eval(temp)
        for i in hot_list:
            title=i["target"]["titleArea"]["text"]#标题
            hot=i["target"]["metricsArea"]["text"]#热度
            excerpt=i["target"]["excerptArea"]["text"]#摘要
            zh_url=i["target"]["link"]["url"]#问题链接
            qid=i["cardId"]
            qidt=re.findall(r"\d+",qid)[0]#问题ID
            res_list.append({'title':title,'heat':hot,'excerpt':excerpt,'url':zh_url,'qid':qidt})
        
        return res_list
        # raise NotImplementedError

    def get_question(self, qid: int) -> dict:
        """
        TODO: Fetch question info by question ID
        :param qid: Question ID
        :return: a dict of question info
        Return Example:
        {
            "created": 1657248657,      # 问题的创建时间
            "followerCount": 5980,      # 问题的关注数量
            "visitCount": 2139067,      # 问题的浏览次数
            "answerCount": 2512         # 问题的回答数量
            "title": "日本前首相安倍      # 问题的标题
                晋三胸部中枪已无生命
                体征 ，嫌疑人被控制，
                目前最新进展如何？背
                后原因为何？",
            "raw": "<p>据央视新闻，        # 问题的详细描述
                当地时间8日，日本前
                首相安倍晋三当天上午
                在奈良发表演讲时中枪
                。据悉，安倍晋三在上
                救护车时还有意。。。",
            "hit_at": 1657264954.3134503  # 请求的时间戳
        }
        """

        # Hint: - Parse JSON, which is embedded in a <script> and contains all information you need.
        #       - After find the element in soup, use `.text` attribute to get the inner text
        #       - Use `json.loads` to convert JSON string to `dict` or `list`
        #       - You may first save the JSON in a file, format it and locate the info you need
        #       - Use `time.time()` to create the time stamp
        #       - Question can be accessed in https://www.zhihu.com/question/<Question ID>
        url=f"https://www.zhihu.com/question/{qid}"
        ans=requests.get(url,headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
            "Cookie":"__snaker__id=LmrTWge3dsfIMOlG; SESSIONID=5S1rL9xj9gsTVuwjOljSJIjgebRluGZW5qrUMFaLJJB; osd=V18cBUKRTUsdS3OJCpJjFzpaC5IS5DI9RjMT_nbaJwRoCzz4QtRrmnxFf4gKHFzxL0qKhkWH1b5KN_x9DipAd88=; JOID=WlwVBEucTkIcQn6KA5NqGjlTCpsf5zs8Tz4Q93fTKgdhCjX1Qd1qk3FGdokDEV_4LkOHhUyG3LNJPv10AylJdsY=; _zap=31894feb-28a8-4f4f-add0-a9b6235e1f69; d_c0=\"AJBfkc6CDBSPThKM7LqcTvOMRl0lqlId3Fk=|1637216980\"; _9755xjdesxxd_=32; YD00517437729195%3AWM_TID=7zmQ3%2BSo%2B2RBURABRRZ%2Fo%2FUdhph4ezVE; _xsrf=3lX64M4nJJEwIZiIQ5lvaHu0oLgLjBhi; __snaker__id=fMQWxv7yCtqR35Qb; q_c1=15b9840395d64e4db7672b6de2bcbce5|1645945047000|1645945047000; __utma=155987696.1706522720.1652132224.1652132224.1652132224.1; __utmz=155987696.1652132224.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); gdxidpyhxdE=YOkTlWaV%5CojSNz55yq9tBEAswa2uhZEfRuNSju1spoA%2FrkkGXdj%2FQ%5Cfe%2BvdBrnlbf8QXhYNJnG5lq1ZHEcixs3Pf7xWB5EmBZN8N4TQzoPO8DaJbRbGZzXSBbXSfTxuhEJAkQWa3eJgHG0x9pEO31mh4Sr9sbO7xiS07U7ojmmcf3UXa%3A1656168682655; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6ee86b5739889a78fd6428cac8ea6d85a828a8fb0c54af8bf84abe273ab97a58ad42af0fea7c3b92a91babfa3f933a6ea9b86c9408392a794b75db3a6bcd0e247abbdbfa5e44fab9cfab2f15bb2e9a093e762f8adbba5c1349cb39ba6b87db6ecfdd5c959f6afad98c579aab08ed7e66282edff86c73fb389b8cce846e9beb7d7d573aa93ac82f76097a6b7ccca60a1efc083e94df1b7a1d9dc4db5abb88bcd3df8b8a2afc85f928bacd3c837e2a3; YD00517437729195%3AWM_NI=SfqBz9f6UeJ4ANis4Wqzt%2FcMicqDaEiD6QrQr7Prdj4suDB8XmUE5wv434JS6qcwFsjpB6gjB31eD5qWx%2BeN%2Bby6VyjMdatTUzy%2BKTc1pbjG3io8YGwuxjN74hnNEURUd2E%3D; captcha_session_v2=2|1:0|10:1656167782|18:captcha_session_v2|88:ZTJvUVRkOEh3VThEdEFyb2w5QnRoc3creHhuMll6T1VmSThJU0wxZFV4V2g4dDV3REZEY1ovWG9jS2FVU1NKVQ==|5a774b2d94c9855378c0074941035fbdba0870fa2147756d0602c4ca0bafb323; z_c0=2|1:0|10:1656167788|4:z_c0|92:Mi4xNlBBNkd3QUFBQUFBa0YtUnpvSU1GQmNBQUFCZ0FsVk5iR3VrWXdCVldmc2xiOVhXbXpZbXBTR3hKdDJNYnVocV9R|b4dc0257c397684b1de3136c4ff15b651e61fea0e095211bd6459fe23a1afdc5; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1657502029,1657810669,1657853584,1657864941; NOT_UNREGISTER_WAITING=1; SESSIONID=wqyTtbysGhAzOCsFHVBnQViM6bOCAzWuelHA1Svug0f; JOID=UVEXC05B0WYSBetdMUr9NzUTm0QkPKsVS3eDL0gAsyxiR64kfFj1sHsJ6lw2Dgr0iSID7SzI_bWGz2B4sBeCAG0=; osd=VlwTAUlG3GIYAuxQNUD6MDgXkUMjMa8fTHCOK0IHtCFmTakjcVz_t3wE7lYxCQfwgyUE4CjC-rKLy2p_txqGCmo=; tst=h; Hm_lpvt_98beee57fd2ef70ccdd5ca52b9740c49=1657874249; KLBRSID=af132c66e9ed2b57686ff5c489976b91|1657874251|1657864939"
        })
        deep=BS(ans.text,'lxml')
        div=deep.find_all("script",id="js-initialData")[0]
        js=str(div)[45:-9]
        jsonneed=json.loads(js)
        qwq=jsonneed['initialState']['entities']['questions'][str(qid)]
        ans_list={}
        ans_list['followerCount']=qwq['followerCount']#关注数
        ans_list['visitCount']=qwq['visitCount']#访问数
        ans_list['created']=qwq['created']#创建时间
        ans_list['title']=qwq['title']#标题
        ans_list['answerCount']=qwq['answerCount']#回答数
        ans_list['raw']=qwq['detail']#详细描述
        ans_list['hit_at']=time.time()
        
        return ans_list
        # raise NotImplementedError

if __name__ == "__main__":
    z = ZhihuCrawler()
    z.watch()
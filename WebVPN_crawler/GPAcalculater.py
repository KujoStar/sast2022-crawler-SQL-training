from selenium.webdriver.remote.webdriver import WebDriver as wd
from selenium.webdriver.edge.service import Service as EdgeService
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as wdw
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains as AC
import selenium
import json
from bs4 import BeautifulSoup as BS

class WebVPN:
    def __init__(self, opt: dict, headless=False):
        self.root_handle = None
        self.driver: wd = None
        self.passwd = opt["password"]
        self.userid = opt["username"]
        self.headless = headless

    def login_webvpn(self):
        """
        Log in to WebVPN with the account specified in `self.userid` and `self.passwd`
        :return:
        """
        d = self.driver
        if d is not None:
            d.close()
        d = selenium.webdriver.Edge()
        d.get("https://webvpn.tsinghua.edu.cn/login")
        username = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item"]//input'
                                   )[0]
        password = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item password-field" and not(@id="captcha-wrap")]//input'
                                   )[0]
        username.send_keys(str(self.userid))
        password.send_keys(self.passwd)
        d.find_element(By.ID, "login").click()
        self.root_handle = d.current_window_handle
        self.driver = d
        return d

    def access(self, url_input):
        """
        Jump to the target URL in WebVPN
        :param url_input: target URL
        :return:
        """
        d = self.driver
        url = By.ID, "quick-access-input"
        btn = By.ID, "go"
        wdw(d, 5).until(EC.visibility_of_element_located(url))
        actions = AC(d)
        actions.move_to_element(d.find_element(*url))
        actions.click()
        actions.\
            key_down(Keys.CONTROL).\
            send_keys("A").\
            key_up(Keys.CONTROL).\
            send_keys(Keys.DELETE).\
            perform()

        d.find_element(*url)
        d.find_element(*url).send_keys(url_input)
        d.find_element(*btn).click()

    def switch_another(self):
        """
        If there are only 2 windows handles, switch to the other one
        :return:
        """
        d = self.driver
        assert len(d.window_handles) == 2
        wdw(d, 5).until(EC.number_of_windows_to_be(2))
        for window_handle in d.window_handles:
            if window_handle != d.current_window_handle:
                d.switch_to.window(window_handle)
                return

    def to_root(self):
        """
        Switch to the home page of WebVPN
        :return:
        """
        self.driver.switch_to.window(self.root_handle)

    def close_all(self):
        """
        Close all window handles
        :return:
        """
        while True:
            try:
                l = len(self.driver.window_handles)
                if l == 0:
                    break
            except selenium.common.exceptions.InvalidSessionIdException:
                return
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.close()

    def login_info(self):
        """
        TODO: After successfully logged into WebVPN, login to info.tsinghua.edu.cn
        :return:
        """

        # Hint: - Use `access` method to jump to info.tsinghua.edu.cn
        #       - Use `switch_another` method to change the window handle
        #       - Wait until the elements are ready, then preform your actions
        #       - Before return, make sure that you have logged in successfully
        try:
            url='info.tsinghua.edu.cn'
            self.access(url)
            self.switch_another()
            d=self.driver
            name=d.find_elements(By.NAME,'userName')
            username=name[0]
            word=d.find_elements(By.NAME,'password')
            password=word[0]
            username.send_keys(str(self.userid))
            password.send_keys(self.passwd+"\n")
            wdw(d,10).until(EC.visibility_of_element_located((By.XPATH,'//*[@id="header"]/div/div/div[1]/div[1]/span')))
            return
        except Exception as E:
            print(E)
            raise NotImplementedError("C2H4,1g1g1g")
            


    def get_grades(self):
        """
        TODO: Get and calculate the GPA for each semester.
        Example return / print:
            2020-秋: *.**
            2021-春: *.**
            2021-夏: *.**
            2021-秋: *.**
            2022-春: *.**
        :return:
        """

        # Hint: - You can directly switch into
        #         `zhjw.cic.tsinghua.edu.cn/cj.cjCjbAll.do?m=bks_cjdcx&cjdlx=zw`
        #         after logged in
        #       - You can use Beautiful Soup to parse the HTML content or use
        #         XPath directly to get the contents
        #       - You can use `element.get_attribute("innerHTML")` to get its
        #         HTML code
        lru='zhjw.cic.tsinghua.edu.cn/cj.cjCjbAll.do?m=bks_cjdcx&cjdlx=zw'
        self.switch_another()
        self.access(lru)
        for x in self.driver.window_handles:
            self.driver.switch_to.window(x)
            things=self.driver.find_element(By.XPATH,"/html")
            soup=BS(things.get_attribute("innerHTML"),"lxml")
            if soup.title.text=='清华大学学生课程学习记录表':
                break
        
        fff=soup.find_all("tbody")
        fff=fff[3]
        beep=BS(str(fff),'lxml')
        ggg=beep.find_all("td")
        iii=[]
        for x in ggg:
            iii.append(x.text.replace("\n","").replace("\t","").replace(" ",""))
    
        lesson_info=[]
        qwq=int(len(iii)/6)
        for cnt in range(0,qwq):
            lesson_info.append({'num':iii[6*cnt],'name':iii[6*cnt+1],'weight':int(iii[6*cnt+2]),'level':iii[6*cnt+3],'GPA':iii[6*cnt+4],'time':iii[6*cnt+5]})

        lesson_info_new=[]
        for i in lesson_info:
            if i['GPA']!='N/A':
                lesson_info_new.append(i)

        res={}
        sem=set([v['time'] for v in lesson_info_new])
        for x in sem:
            total_weight=0
            total_GPA=0
            for y in lesson_info_new:
                if y['time']==x:
                    total_weight+=y['weight']
                    total_GPA+=y['weight']*float(y['GPA'])
            res[x]=round(total_GPA/total_weight,2)
            
        return res
        # raise NotImplementedError("1919810")

if __name__ == "__main__":
    # TODO: Write your own query process
    try:
        with open ("./GPAsettings.json") as f:
            opt=json.load(f)

        tensor=WebVPN(opt)
        tensor.login_webvpn()
        tensor.login_info()
        final=tensor.get_grades()
        print(final)
    except Exception:
        raise NotImplementedError("114514")
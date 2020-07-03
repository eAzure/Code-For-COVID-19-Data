# 获取网页中包含数据链接的列表信息
# 1
import requests
import re
import json
from bs4 import BeautifulSoup
def getOriHtmlText(url,code='utf-8'):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
        r=requests.get(url,timeout=30,headers=headers)
        r.raise_for_status()
        r.encoding=code
        return r.text
    except:
        return "There are some errors when get the original html!"
def getTheList(url):
    html=getOriHtmlText(url)
    soup=BeautifulSoup(html,'html.parser')
    # script=soup.find_all('script',{"id":"getListByCountryTypeService2true"})
    # print(script.find(''))
    htmlBodyText=soup.body.text
    # 获取国家数据
    worldDataText=htmlBodyText[htmlBodyText.find('window.getListByCountryTypeService2true = '):]
    worldDataStr = worldDataText[worldDataText.find('[{'):worldDataText.find('}catch')]
    worldDataJson=json.loads(worldDataStr)
    with open("../data/worldData.json","w") as f:
        json.dump(worldDataJson,f)
        print("写入国家数据文件成功！")
    # 获取各省份数据
    provinceDataText = htmlBodyText[htmlBodyText.find('window.getAreaStat = '):]
    provinceDataStr = provinceDataText[provinceDataText.find('[{'):provinceDataText.find('}catch')]
    provinceDataJson=json.loads(provinceDataStr)
    with open("../data/provinceData.json","w") as f:
        json.dump(provinceDataJson,f)
        print("写入省份数据文件成功！")

getTheList("https://ncov.dxy.cn/ncovh5/view/pneumonia")
# 处理所获得的含链接的列表，获取真实所对应的数据
# 2
import json
import requests
import time
def deal_worlddatalist():
    with open("../data/worldData.json",'r') as f:
        worldDataJson=json.load(f)
    # print(len(worldDataJson))
    # print(worldDataJson)
    for i in range(0,len(worldDataJson)):
        print(worldDataJson[i]['provinceName']+" "+worldDataJson[i]['countryShortCode']+" "+worldDataJson[i]['countryFullName']+" "+worldDataJson[i]['statisticsData'])
    return worldDataJson
def get_the_world_data():
    # 获取每个国家对应的json
    worldDataJson=deal_worlddatalist()
    # 记录错误数量
    errorNum=0
    for i in range(0,len(worldDataJson)):
        provinceName=worldDataJson[i]['provinceName']
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            }

            r = requests.get(worldDataJson[i]['statisticsData'], timeout=30, headers=headers)
            r.raise_for_status()
            r.encoding = 'utf-8'
            everCountryDataJson = json.loads(r.text)
            toWriteFilePath="../data/worldData/"+provinceName+".json"
            with open(toWriteFilePath,'w') as file:
                json.dump(everCountryDataJson, file)
            print(provinceName + " 数据得到！")
            time.sleep(10)
        except:
            errorNum+=1
            print("在获取 "+provinceName+" 数据时出错！")
    print("各国数据获取完成！")
    print("错误数据量为："+str(errorNum))

# 处理各省数据列表
def deal_provincedatalist():
    with open("../data/provinceData.json",'r') as f:
        provinceDataJson=json.load(f)
    for i in range(0,len(provinceDataJson)):
        print(provinceDataJson[i]['provinceName']+" "+provinceDataJson[i]['provinceShortName']+" "+provinceDataJson[i]['statisticsData'])
    return provinceDataJson
# 获取各个省份对应的json
def get_the_province_data():
    provinceDataJson=deal_provincedatalist()
    # 统计出现爬取错误的数据数量
    errorNum = 0
    for i in range(0,len(provinceDataJson)):
        provinceName=provinceDataJson[i]['provinceName']
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            }
            r=requests.get(provinceDataJson[i]['statisticsData'],timeout=30,headers=headers)
            r.raise_for_status()
            r.encoding='utf-8'
            everProvinceDataJson=json.loads(r.text)

            toWriteFilePath="../data/provinceData/"+provinceName+".json"
            with open(toWriteFilePath,'w') as file:
                json.dump(everProvinceDataJson,file)
            print(provinceName+" 数据得到！")
            time.sleep(15)
        except:
            errorNum+=1
            print("在获取 "+provinceName+" 数据时出错")
    print("各省份数据获取完成！")
    print("错误数据量为："+str(errorNum))

get_the_world_data()
get_the_province_data()
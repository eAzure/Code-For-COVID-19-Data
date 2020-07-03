# coding=UTF-8
# 将json数据写入数据库
# 3

import json
import pymysql
import pandas as pd
nameMap = {'毛里求斯':'Mauritius','圣皮埃尔和密克隆群岛':'St. Pierre and Miquelon','安圭拉':'Anguilla','荷兰加勒比地区':'Caribbean Netherlands','圣巴泰勒米岛':'Saint Barthelemy','英属维尔京群岛':'British Virgin Is.','科摩罗':'Comoros','蒙特塞拉特':'Montserrat','塞舌尔':'Seychelles','特克斯和凯科斯群岛':'Turks and Caicos Is.','梵蒂冈':'Vatican','圣其茨和尼维斯':'Saint Kitts and Nevis','库拉索岛':'Curaçao','多米尼克':'Dominica','圣文森特和格林纳丁斯':'St. Vin. and Gren.','斐济':'Fiji','圣卢西亚':'Saint Lucia','北马里亚纳群岛联邦':'N. Mariana Is.','格林那达':'Grenada','安提瓜和巴布达':'Antigua and Barb.','列支敦士登':'Liechtenstein','圣马丁岛':'Saint Martin','法属波利尼西亚':'Fr. Polynesia','美属维尔京群岛':'U.S. Virgin Is.','荷属圣马丁':'Sint Maarten','巴巴多斯':'Barbados','开曼群岛':'Cayman Is.','摩纳哥':'Monaco','阿鲁巴':'Aruba','特立尼达和多巴哥':'Trinidad and Tobago','钻石公主号邮轮':'Princess','瓜德罗普岛':'Guadeloupe','关岛':'Guam','直布罗陀':'Gibraltar','马提尼克':'Martinique','马耳他':'Malta','法罗群岛':'Faeroe Is.','圣多美和普林西比':'São Tomé and Principe','安道尔':'Andorra','根西岛':'Guernsey','泽西岛':'Jersey','佛得角':'Cape Verde','马恩岛':'Isle of Man','留尼旺':'Reunion','圣马力诺':'San Marino','马尔代夫':'Maldives','马约特':'Mayotte','巴林':'Bahrain','新加坡': 'Singapore Rep.', '多米尼加': 'Dominican Rep.', '巴勒斯坦': 'Palestine', '巴哈马': 'The Bahamas', '东帝汶': 'East Timor', '阿富汗': 'Afghanistan', '几内亚比绍': 'Guinea Bissau', '科特迪瓦': "Côte d'Ivoire", '锡亚琴冰川': 'Siachen Glacier', '英属印度洋领土': 'Br. Indian Ocean Ter.', '安哥拉': 'Angola', '阿尔巴尼亚': 'Albania', '阿联酋': 'United Arab Emirates', '阿根廷': 'Argentina', '亚美尼亚': 'Armenia', '法属南半球和南极领地': 'French Southern and Antarctic Lands', '澳大利亚': 'Australia', '奥地利': 'Austria', '阿塞拜疆': 'Azerbaijan', '布隆迪共和国': 'Burundi', '比利时': 'Belgium', '贝宁': 'Benin', '布基纳法索': 'Burkina Faso', '孟加拉国': 'Bangladesh', '保加利亚': 'Bulgaria', '波黑': 'Bosnia and Herz.', '白俄罗斯': 'Belarus', '伯利兹': 'Belize', '百慕大': 'Bermuda', '玻利维亚': 'Bolivia', '巴西': 'Brazil', '文莱': 'Brunei', '不丹': 'Bhutan', '博茨瓦纳': 'Botswana', '中非共和国': 'Central African Rep.', '加拿大': 'Canada', '瑞士': 'Switzerland', '智利': 'Chile', '中国': 'China', '象牙海岸': 'Ivory Coast', '喀麦隆': 'Cameroon', '刚果（金）': 'Dem. Rep. Congo', '刚果（布）': 'Congo', '哥伦比亚': 'Colombia', '哥斯达黎加': 'Costa Rica', '古巴': 'Cuba', '北塞浦路斯': 'N. Cyprus', '塞浦路斯': 'Cyprus', '捷克': 'Czech Rep.', '德国': 'Germany', '吉布提': 'Djibouti', '丹麦': 'Denmark', '阿尔及利亚': 'Algeria', '厄瓜多尔': 'Ecuador', '埃及': 'Egypt', '厄立特里亚': 'Eritrea', '西班牙': 'Spain', '爱沙尼亚': 'Estonia', '埃塞俄比亚': 'Ethiopia', '芬兰': 'Finland', '斐': 'Fiji', '福克兰群岛': 'Falkland Islands', '法国': 'France', '加蓬': 'Gabon', '英国': 'United Kingdom', '格鲁吉亚': 'Georgia', '加纳': 'Ghana', '几内亚': 'Guinea', '冈比亚': 'Gambia', '赤道几内亚': 'Eq. Guinea', '希腊': 'Greece', '格陵兰': 'Greenland', '危地马拉': 'Guatemala', '法属圭亚那': 'French Guiana', '圭亚那': 'Guyana', '洪都拉斯': 'Honduras', '克罗地亚': 'Croatia', '海地': 'Haiti', '匈牙利': 'Hungary', '印度尼西亚': 'Indonesia', '印度': 'India', '爱尔兰': 'Ireland', '伊朗': 'Iran', '伊拉克': 'Iraq', '冰岛': 'Iceland', '以色列': 'Israel', '意大利': 'Italy', '牙买加': 'Jamaica', '约旦': 'Jordan', '日本': 'Japan', '哈萨克斯坦': 'Kazakhstan', '肯尼亚': 'Kenya', '吉尔吉斯斯坦': 'Kyrgyzstan', '柬埔寨': 'Cambodia', '韩国': 'Korea', '科索沃': 'Kosovo', '科威特': 'Kuwait', '老挝': 'Lao PDR', '黎巴嫩': 'Lebanon', '利比里亚': 'Liberia', '利比亚': 'Libya', '斯里兰卡': 'Sri Lanka', '莱索托': 'Lesotho', '立陶宛': 'Lithuania', '卢森堡': 'Luxembourg', '拉脱维亚': 'Latvia', '摩洛哥': 'Morocco', '摩尔多瓦': 'Moldova', '马达加斯加': 'Madagascar', '墨西哥': 'Mexico', '北马其顿': 'Macedonia', '马里': 'Mali', '缅甸': 'Myanmar', '黑山': 'Montenegro', '蒙古': 'Mongolia', '莫桑比克': 'Mozambique', '毛里塔尼亚': 'Mauritania', '马拉维': 'Malawi', '马来西亚': 'Malaysia', '纳米比亚': 'Namibia', '新喀里多尼亚': 'New Caledonia', '尼日尔': 'Niger', '尼日利亚': 'Nigeria', '尼加拉瓜': 'Nicaragua', '荷兰': 'Netherlands', '挪威': 'Norway', '尼泊尔': 'Nepal', '新西兰': 'New Zealand', '阿曼': 'Oman', '巴基斯坦': 'Pakistan', '巴拿马': 'Panama', '秘鲁': 'Peru', '菲律宾': 'Philippines', '巴布亚新几内亚': 'Papua New Guinea', '波兰': 'Poland', '波多黎各': 'Puerto Rico', '朝鲜': 'Dem. Rep. Korea', '葡萄牙': 'Portugal', '巴拉圭': 'Paraguay', '卡塔尔': 'Qatar', '罗马尼亚': 'Romania', '俄罗斯': 'Russia', '卢旺达': 'Rwanda', '西撒哈拉': 'W. Sahara', '沙特阿拉伯': 'Saudi Arabia', '苏丹': 'Sudan', '南苏丹': 'S. Sudan', '塞内加尔': 'Senegal', '所罗门群岛': 'Solomon Is.', '塞拉利昂': 'Sierra Leone', '萨尔瓦多': 'El Salvador', '索马里兰': 'Somaliland', '索马里': 'Somalia', '塞尔维亚': 'Serbia', '苏里南': 'Suriname', '斯洛伐克': 'Slovakia', '斯洛文尼亚': 'Slovenia', '瑞典': 'Sweden', '斯威士兰': 'Swaziland', '叙利亚': 'Syria', '乍得': 'Chad', '多哥': 'Togo', '泰国': 'Thailand', '塔吉克斯坦': 'Tajikistan', '土库曼斯坦': 'Turkmenistan', '特里尼达和多巴哥': 'Trinidad and Tobago', '突尼斯': 'Tunisia', '土耳其': 'Turkey', '坦桑尼亚': 'Tanzania', '乌干达': 'Uganda', '乌克兰': 'Ukraine', '乌拉圭': 'Uruguay', '美国': 'United States', '乌兹别克斯坦': 'Uzbekistan', '委内瑞拉': 'Venezuela', '越南': 'Vietnam', '瓦努阿图': 'Vanuatu', '西岸': 'West Bank', '也门共和国': 'Yemen', '南非': 'South Africa', '赞比亚共和国': 'Zambia', '津巴布韦': 'Zimbabwe'}
# 将各国json数据写入数据库
def importWorldJsonToDB():
    # 建立数据库连接
    db = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="password",
        database="epidemic"
    )
    # 使用cursor()方法创建一个游标对象cursor
    cursor=db.cursor()
    # 不写增量添加了，因为数据量也不是很大并且前面的操作都为考虑增量，所以每一次都直接删了重新导吧
    deleteSql="truncate countrydata"
    try:
        cursor.execute(deleteSql)
        db.commit()
        print("删除国家数据成功！进行重新导入！")
    except:
        print("删除国家数据时出错！")
        db.rollback()
    with open("../data/worldData.json",'r') as f:
        worldDataJson=json.load(f)
    # 批量插入的数据集合
    insertValue=[]
    # 所插入的主键记录
    dataCount=1
    for i in range(0, len(worldDataJson)):
        # 获取每一个国家的名称，并打开其对应的json文件
        countryName=worldDataJson[i]['provinceName']
        countryShortCode=worldDataJson[i]['countryShortCode']
        continent=worldDataJson[i]['continents']
        countryFullName=nameMap[worldDataJson[i]['provinceName']]
        countryJsonPath="../data/worldData/"+countryName+".json"
        with open(countryJsonPath) as f:
            countryJson=json.load(f)
        for j in range(0,len(countryJson['data'])):
            tupleData=()
            tupleData+=(
                dataCount,countryJson['data'][j]['confirmedCount'],countryJson['data'][j]['confirmedIncr'],
                countryJson['data'][j]['curedCount'],countryJson['data'][j]['curedIncr'],countryJson['data'][j]['currentConfirmedCount'],
                countryJson['data'][j]['currentConfirmedIncr'],countryJson['data'][j]['dateId'],countryJson['data'][j]['deadCount'],
                countryJson['data'][j]['deadIncr'],countryJson['data'][j]['suspectedCount'],countryJson['data'][j]['suspectedCountIncr'],
                countryName,countryShortCode,continent,countryFullName
            )
            insertValue.append(tupleData)
            dataCount+=1
    insertSql="INSERT INTO countrydata (id,confirmedCount,confirmedIncr,curedCount,curedIncr,currentConfirmedCount,currentConfirmedIncr,dateId,deadCount,deadIncr,suspectedCount,suspectedCountIncr,countryName,countryShortCode,continent,countryFullName) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    # 执行数据插入
    try:
        cursor.executemany(insertSql,insertValue)
        db.commit()
        print("插入国家数据成功！")
    except:
        print("插入国家数据失败！")
        db.rollback()
    # 关闭连接
    cursor.close()
    db.close()
# 将各省数据json数据写入数据库
def importProvinceJsonToDB():
    # 建立数据库连接
    db=pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="password",
        database="epidemic"
    )
    # 使用cursor()方法创建一个游标对象cursor
    cursor=db.cursor()
    # 同上，不写增量添加了，每一次直接删除所有数据重新添加
    deleteSql="truncate china_provincedata"
    try:
        cursor.execute(deleteSql)
        db.commit()
        print("删除省份数据成功！进行重新导入！")
    except:
        print("删除省份数据时出错！")
        db.rollback()
    with open("../data/provinceData.json",'r') as f:
        provinceDataJson=json.load(f)
    #批量插入的数据集合
    insertValue=[]
    # 所插入的主键记录
    dataCount=1
    for i in range(0,len(provinceDataJson)):
        # 获取每一个省份的名称，并打开其对应的json文件
        provinceName=provinceDataJson[i]['provinceName']
        provinceShortName=provinceDataJson[i]['provinceShortName']
        provinceJsonPath="../data/provinceData/"+provinceName+".json"
        with open(provinceJsonPath) as f:
            provinceJson=json.load(f)
        for j in range(0,len(provinceJson['data'])):
            tupleData=()
            tupleData+=(
                dataCount,provinceJson['data'][j]['confirmedCount'],provinceJson['data'][j]['confirmedIncr'],
                provinceJson['data'][j]['curedCount'],provinceJson['data'][j]['curedIncr'],
                provinceJson['data'][j]['currentConfirmedCount'],provinceJson['data'][j]['currentConfirmedIncr'],
                provinceJson['data'][j]['dateId'],provinceJson['data'][j]['deadCount'],provinceJson['data'][j]['deadIncr'],
                provinceJson['data'][j]['suspectedCount'],provinceJson['data'][j]['suspectedCountIncr'],
                provinceName,provinceShortName
            )
            insertValue.append(tupleData)
            dataCount+=1
    insertSql="INSERT INTO china_provincedata(id,confirmedCount,confirmedIncr,curedCount,curedIncr,currentConfirmedCount,currentConfirmedIncr,dateId,deadCount,deadIncr,suspectedCount,suspectedCountIncr,provinceName,provinceShortName) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    # 执行数据插入
    try:
        cursor.executemany(insertSql,insertValue)
        db.commit()
        print("插入省份数据成功！")
    except:
        print("插入省份数据失败！")
        db.rollback()
    # 关闭连接
    cursor.close()
    db.close()
# 反转名字字典
def inverse():
    print("hello")
    print(dict([(v,k) for (k,v) in nameMap.items()]))
def importWorldConToDB():
    # 建立数据库连接
    db = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="password",
        database="epidemic"
    )
    # 使用cursor()方法创建一个游标对象cursor
    cursor = db.cursor()
    # 不写增量添加了，因为数据量也不是很大并且前面的操作都为考虑增量，所以每一次都直接删了重新导吧
    deleteSql = "truncate world_total_data"
    try:
        cursor.execute(deleteSql)
        db.commit()
        print("删除世界数据成功！进行重新导入！")
    except:
        print("删除世界数据时出错！")
        db.rollback()
    # searchSql = "select distinct(dateId) from countrydata"
    # try:
    #     cursor.execute(searchSql)
    #     db.commit()
    #     print("搜索日期数据成功！")
    # except:
    #     print("搜索日期数据时出错！")
    #     db.rollback()
    # searchList=cursor.fetchall()
    # dateList=[]
    # for dateData in searchList:
    #     dateList.append(dateData[0])
    # print(dateList)
    searchSql="select sum(confirmedCount),sum(confirmedIncr),sum(curedCount),sum(curedIncr),sum(currentConfirmedCount),sum(currentConfirmedIncr),dateId,sum(deadCount),sum(deadIncr),sum(suspectedCount),sum(suspectedCountIncr) from countrydata group by dateId order by dateId"
    cursor.execute(searchSql)
    searchList=cursor.fetchall()
    dataList=[]
    i=1
    for data in searchList:
        temp=()
        temp+=(i,int(data[0]),int(data[1]),int(data[2]),int(data[3]),int(data[4]),int(data[5]),int(data[6]),int(data[7]),int(data[8]),int(data[9]),int(data[10]))
        dataList.append(temp)
        i+=1
    #print(dataList)
    # 开始插入数据
    insertSql="INSERT INTO world_total_data (id,confirmedCount,confirmedIncr,curedCount,curedIncr,currentConfirmedCount,currentConfirmedIncr,dateId,deadCount,deadIncr,suspectedCount,suspectedCountIncr) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    # 执行数据插入
    try:
        cursor.executemany(insertSql, dataList)
        db.commit()
        print("插入世界总体数据成功！")
    except:
        print("插入世界总体数据失败！")
        db.rollback()
    # 关闭连接
    cursor.close()
    db.close()



importWorldJsonToDB()
importProvinceJsonToDB()
importWorldConToDB()
#inverse()

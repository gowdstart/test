__author__ = 'happy'
#coding=utf-8
import time
import os
import sys
from myprint import myprint
import MySQLdb

class process_log(object):
	def __init__(self,kafka=""):
		print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		now=int(time.time())
		self.lastmin=now-now%60
		self.qingsuandir={}
		self.kafka=kafka

	def getmsginfo(self,msg):
		result={}
		msglist=msg.split()
		result['haip']=msglist[0]
		result['backend']=msglist[5].split("/")[0]
		result['code']=msglist[7]
		#如果进入front直接进行跳转时，则忽略此条日志，此时日志中出现NOSERV，code为301或302，且backend=frontend
		if "/<NOSRV> " in msg and result['backend'] == msglist[4]:
			return {}
		result['wait_server']=int(msglist[6].split("/")[3])
		result['total_request']=int(msglist[6].split("/")[4])
		result['datasize']=int(msglist[8])
		#某些指定的backend，可能有多种domain，这种backend，domain根据需求设置
		if result['backend'] in ['static']:
			result['domain']="*"
		elif result['backend']=='merchant-shop':
			result['domain']='[shangjia name].yhd.com'
		else:
			result['domain']=msg[msg.find("{"):].split("|")[1]
		# 获取PATH
		# path=msg[msg[:-1].rfind('"'):]
		# if "?" in path or " HTTP/" in path:
		# 	if " ?" in path:
		# 		#若出现"GET ?tracker_u=10449911441 HTTP/1.1"这种直接以?开头的URL，则PATH为/
		# 		result['PATH']="/"
		# 	else:
		# 		result['PATH']=path[path.find("/"):path.find("?")].split()[0]
		# else:
		# 	result['PATH']=""
		return result

	def qingsuan(self,eachlog):
		"""将backend存入qingsuandir"""
		backend=eachlog['backend']
		haip=eachlog['haip']
		domain=eachlog['domain']
		code=eachlog['code']
		if not self.qingsuandir.has_key(backend):
			self.qingsuandir[backend]={}
		if not self.qingsuandir[backend].has_key(haip):
			self.qingsuandir[backend][haip]={}
		if not self.qingsuandir[backend][haip].has_key(domain):
			self.qingsuandir[backend][haip][domain]={}
		self.qingsuandir[backend][haip][domain]["request_times"]=self.qingsuandir[backend][haip][domain].get("request_times",0)+1
		self.qingsuandir[backend][haip][domain][code]=self.qingsuandir[backend][haip][domain].get(code,0)+1
		self.qingsuandir[backend][haip][domain]["datasize"]=self.qingsuandir[backend][haip][domain].get("datasize",0)+eachlog['datasize']
		self.qingsuandir[backend][haip][domain]["wait_server"]=self.qingsuandir[backend][haip][domain].get("wait_server",0)+eachlog['wait_server']
		self.qingsuandir[backend][haip][domain]["total_request"]=self.qingsuandir[backend][haip][domain].get("total_request",0)+eachlog['total_request']

	def inserttoDB(self):
		"""将self.qingsuandir的结果存入数据库中，并且重置self.resultlist"""
		startinsert=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		now=int(time.time())
		dbnumber=0
		backendnum=0
		for backend in self.qingsuandir:
			backendnum+=1
			for haip in self.qingsuandir[backend]:
				for domain in self.qingsuandir[backend][haip]:
					request_times=self.qingsuandir[backend][haip][domain]["request_times"]
					wait_server=self.qingsuandir[backend][haip][domain]["wait_server"]/request_times
					total_request=self.qingsuandir[backend][haip][domain]["total_request"]/request_times
					datasize=self.qingsuandir[backend][haip][domain]["datasize"]/request_times
					code200=self.qingsuandir[backend][haip][domain].get('200',0)
					code206=self.qingsuandir[backend][haip][domain].get('206',0)
					code301=self.qingsuandir[backend][haip][domain].get('301',0)
					code302=self.qingsuandir[backend][haip][domain].get('302',0)
					code304=self.qingsuandir[backend][haip][domain].get('304',0)
					code306=self.qingsuandir[backend][haip][domain].get('306',0)
					code400=self.qingsuandir[backend][haip][domain].get('400',0)
					code403=self.qingsuandir[backend][haip][domain].get('403',0)
					code404=self.qingsuandir[backend][haip][domain].get('404',0)
					code502=self.qingsuandir[backend][haip][domain].get('502',0)
					code503=self.qingsuandir[backend][haip][domain].get('503',0)
					code504=self.qingsuandir[backend][haip][domain].get('504',0)
					con=MySQLdb.connect(user='root',db='kehan2',passwd='yhd@123',host='10.4.2.154',charset='utf8')
					cur=con.cursor()
					insertformat="insert into hainfo(time,backend,haip,domain,request_times,wait_server,total_request,datasize,code200,code206,code301,code302,code304,code306,code400,code403,code404,code502,code503,code504) values(%d,'%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"
					insertvalue=(now,backend,haip,domain,request_times,wait_server,total_request,datasize,code200,code206,code301,code302,code304,code306,code400,code403,code404,code502,code503,code504)
					insertcmd= insertformat % insertvalue
					cur.execute(insertcmd)
					dbnumber+=1
		endinsert=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		self.writelog("backends: %5d insert to db lines:%5d start:%s end:%s" % (backendnum,dbnumber,startinsert,endinsert))
		self.writelog(str(self.kafka.offsets()),logfile="offsets.log")

	def writelog(self,info,logfile='./kehan.log'):
		now=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		open(logfile,"a").write(now+" : "+info+"\n")

	def writeerrorlog(self,info):
		logfile="./error.log"
		now=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		open(logfile,"a").write(now+" : "+info+"\n")

	def writeqingsuanlog(self,info):
		logfile="./qingsuan.log"
		now=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		open(logfile,"a").write(now+" : "+info+"\n")

	def test(self,msg):
		print self.getmsginfo(msg)

	def process(self,msg):
		"""整理每条log的字段，存入resultlist中，当前分钟与self.lastmin不一致时，进行一次清算
		调用qingsuan()与inserttoDB()"""
		result=""
		for eachlog in msg.split("\n"):
			try:
				if eachlog.startswith("10.63"):
					continue
				result=self.getmsginfo(eachlog)
			except (IndexError,ValueError):
				#若出现无法解析的字符串，则进行日志记录，以供今后分析，并且排除一些已知的无效日志格式
				if " CK: " in eachlog \
				or eachlog == "" \
				or ":1080 (stats/HTTP)" in eachlog \
				or "<BADREQ>" in eachlog \
				or "stats stats/" in eachlog \
				or "]: Stopping proxy " in eachlog \
				or "]: Stopping backend " in eachlog \
				or "]: Stopping frontend " in eachlog \
				or "]: frontend " in eachlog \
				or "]: Pausing frontend " in eachlog \
				or "]: Pausing proxy " in eachlog \
				or "]: Some proxies refused to pause" in eachlog \
				or "]: proxy stats failed to enter pause mode." in eachlog \
				or "]: Proxy " in eachlog \
				or "]: switch ip " in eachlog \
				or "]: Server " in eachlog \
				or ", reason: Layer7 " in eachlog \
				or ", reason: Layer4 " in eachlog \
				or " SSL handshake failure" in eachlog \
				or "has no server available" in eachlog \
				or "]: Health check for server " in eachlog:
					continue
				else:
					self.writeerrorlog(eachlog)
			if result:
				self.qingsuan(result)
		now=int(time.time())
		nowmin=now-now%60
		if nowmin != self.lastmin:
			self.inserttoDB()
			self.qingsuandir={}
			self.lastmin=nowmin

if __name__=="__main__":
	pl=process_log()
	#pl.process('10.4.5.157 haproxy[22299]: 172.17.37.121:44733 [04/Mar/2015:17:06:42.169] yhd_pub front-myyhd/10.4.22.35 0/0/0/26/26 200 457 - - ---- 5109/2/1/0/0 0/0 {1|interface.m.yhd.com|106.110.48.128|} "POST /myyhdmobile/order/getLogisticByToken.do? HTTP/1.1"')
	#pl.process('10.4.5.157 haproxy[22299]: 172.17.17.143:3784 [04/Mar/2015:17:06:42.169] yhd_web front-detail/10.4.4.147 0/0/0/26/26 200 400 - - ---- 5110/26/10/0/0 0/0 {Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36|item-home.yhd.com|114.248.223.77|http://item.yhd.com/item/10967051?tc=3.0.5.10967051.24&tp=51.%E6%83%A0%E6%B0%8Fs-26.124.0.107.Kj`2YC4-11-ADNOV} "GET /item/ajax/ajaxCommonIntimeJsonp.do?callback=jsonp1425460053845&merchantID=2&mainProductID=9121984&productID=9121984&productMercantID=10967051&topCategoryID=5117&pageType=0&prodType=0&isYihaodian=1&combSubPmInfoId=0&combSubProductId=0&backCategoryId=953385&landingpageID=0&ProvinceID=2 HTTP/1.1"')
	pl.test('10.4.1.157 haproxy[4137]: 172.17.37.93:23312 [05/Mar/2015:11:17:43.061] yhd_it5 mobile-website-map/10.4.0.51 0/0/0/0/0 400 130 - - ---- 86/86/2/0/0 0/0 {Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53|m.yhd.com|219.146.12.122|http://www.yihaodian.com} "GET ?tracker_u=10449911441 HTTP/1.1"')
	print pl.qingsuandir
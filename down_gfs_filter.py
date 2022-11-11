# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 09:25:44 2022

@author: schao
"""
import os
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor,as_completed
import pygrib as pg

down_var = ['4LFTX','ABSV','ACPCP','ALBDO','APCP','CAPE','CICEP','CIN','CLWMR','CNWAT',
			'CRAIN','CSNOW','CWORK','DLWRF','DSWRF','DZDT','FLDCP','FRICV','GFLUX',
			'GRLE','GUST','HCDC','HGT','HINDEX','HPBL','ICEC','ICETMP','ICMR','LAND',
			'LCDC','LHTFL','MCDC','O3MR','PLPL','POT','PRES','PRMSL','REFC','REFD',
			'RH','RWMR','SFCR','SNMR','SOTYP','SPFH','SUNSD','TCDC','TMP','TOZNE',
			'UGRD','ULWRF','USWRF','VEG','VGRD','VIS','VRATE','VVEL','VWSH','WILT']

def checkdir(path):
	if not os.path.exists(path):
		os.makedirs(path)

def domain_down(leftlon=70,rigthlon=150,toplat=80,bottomlat=-10):
	'''
	purpose:模拟区域经纬度设置
	'''
	domain = {'leftlon':leftlon,'rightlon':rigthlon,
				  'toplat':toplat,'bottomlat':bottomlat}
	return domain

def read_grib(gribFile):
	'''
	purpose:基于pygrib库检查变量是否在文件中
	'''
	try:
		grbs = pg.open(gribFile)# 所有变量
		lastMes = str(list(grbs)[-1])
		if 'Vertical speed shear' in lastMes and 'potentialVorticity' in lastMes and '2.147485648' in lastMes:
			return 0
	except Exception as e:
		print(e)
		return 1
	return 1

def down_file(thisTime,htime,thour,tdelta):
	'''
	propose:获取网页下载gfs-filter数据的网址
	'''
	URL = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?file='
	gfsName = 'gfs.t{htime}z.pgrb2.0p25.f{HH}'
	filePath = []
	domain = domain_down(leftlon=70,rigthlon=150,toplat=80,bottomlat=-10)
	for i in range(0,thour,tdelta):
		HH = str(i).zfill(3)
		webPath = URL+gfsName.format(htime=htime,HH = HH)+'&all_lev=on&all_var=on&subregion=&leftlon={leftlon}\
&rightlon={rightlon}&toplat={toplat}&bottomlat={bottomlat}&dir=%2Fgfs.\
{thisTime}%2F{htime}%2Fatmos'.format(**domain,htime=htime,thisTime=thisTime)
		filePath.append(webPath)
	return filePath

def before_down_file(beforedays,htime,thisTime):
	'''
	purpose:下载前几天的数据文件网址
	'''
	bfFilePath = []
	dnow = datetime.strptime(thisTime, '%Y%m%d')
	for i in range(beforedays):
		thisday = dnow - timedelta(days=i+1)
		thisTime = thisday.strftime('%Y%m%d')
		filePath = down_file(thisTime, htime, 24, 6)
		bfFilePath.extend(filePath)
	return bfFilePath

def check_down(webfile,file_down,cmd):
	'''
	purpose:检查文件是否存在及下载完成
	'''
	if not os.path.exists(file_down):
		os.system(cmd)
		isKey = read_grib(file_down)
	else:
		print(file_down + ' exists')
		isKey = read_grib(file_down)
	# 判断下载的文件是否完成
	num = 0
	while isKey == 1:
		os.remove(file_down)
		os.system(cmd)
		isKey = read_grib(file_down)
		num = num+1
		if num > 5:
			break

def down(webfile,outdir):
	'''
	purpose:文件下载
	'''
	thisTime = webfile.split('%')[-3].split('.')[1]
	htime = webfile.split('%')[-2][2:]
	HH = webfile.split('&')[0][-3:]
	fileName = 'gfs.t{htime}z.pgrb2.0p25.f{thisTime}-{HH}'.format(htime=htime,
							   thisTime=thisTime,HH=HH)
	cmd = '''wget  --no-check-certificate -c --timeout=100 --retry-connrefused --tries=300 --limit-rate=100k "%s" -O %s/%s '''%(webfile,outdir,fileName)
	file_down = outdir+'/'+fileName
	check_down(webfile,file_down, cmd)

def main_thread(thisTime,htime,beforeDays,thour,tdelta,outPath):
	'''
	purpose:基于多线程下载gfs-fileter主程序
	'''
	filePath = down_file(thisTime, htime, thour, tdelta)
	beforeFilePath = before_down_file(beforeDays,htime,thisTime)
	tFilePath = []
	tFilePath.extend(filePath);tFilePath.extend(beforeFilePath)
	outdir = outPath+'/'+thisTime+'/'+htime+'/atmos'
	checkdir(outdir)
	outdirList = [outdir]*len(tFilePath)
	with ThreadPoolExecutor(max_workers=24) as executer:
		all_work = executer.map(down,tFilePath,outdirList)
		for future in as_completed(all_work):
			work = all_work[future]
			try:
				data = future.result()
			except Exception as e:
				print('%r generated an exception: %s' % (work, e))
			else:
				print(data,future)

if __name__ == '__main__':
	thisTime = '20221109'
	thisTime = datetime.now().strftime('%Y%m%d')
	htime = '00'
	thour = 12
	tdelta = 6
	outPath = './'
	beforeDays = 0
	main_thread(thisTime, htime, beforeDays, thour, tdelta, outPath)
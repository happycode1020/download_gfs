# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 12:15:54 2022

@author: schao
"""
import os
import requests
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor,as_completed,ProcessPoolExecutor

def checkdir(path):
	if not os.path.exists(path):
		os.makedirs(path)

def get_file_path(thistime,htime,thour,tdelta):
	'''
	purpose:获取文件的全路径
	args:
		thistime:下载的时间：20220831
		htime:下载数据的某个时刻，(00 06 12 18)
		thour:下载的总小时数
		tdelta:小时的时间间隔
	'''
	URL = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod'
	path = 'gfs.{thistime}/{htime}/atmos'
	file_name = 'gfs.t{htime}z.pgrb2.0p25.f{HH}'
	path_f = path.format(thistime=thistime,htime=htime)
	file_path = []
	for i in range(0,thour,tdelta):
		HH = str(i).zfill(3)
		name = file_name.format(htime=htime,HH=HH)
		dpath = URL+'/'+path_f+'/'+name
		file_path.append(dpath)

	return file_path

def befor_down_file(beforedays,htime,thistime):
	'''
	purpose:获取前几天的gfs数据下载全路径
	args:
		beforedays:天数
		htime:下载数据的时刻，‘00’ ’06‘ ’12‘ ’18‘
		thistime:此时刻时间 ‘20220902’
	return:
		返回前几天的文件列表
	'''
	bf_file_path = []
	dnow = datetime.strptime(thistime,'%Y%m%d')
	for i in range(beforedays):
		thisday = dnow-timedelta(days=i+1)
		thistime = thisday.strftime('%Y%m%d')
		file_path = get_file_path(thistime, htime, 24, 6)
		bf_file_path.extend(file_path)
	return bf_file_path

def down_load(file,outdir):
	'''
	purpose:下载数据
	args:
		file:下载的文件网址；
		outdir:文件的下载路径
	'''
	file_name = file.split('/')[-1]
	file_name_1 = file_name[:-3]
	file_name_2 = file_name[-3:]
	thisday = file.split('/')[-4].split('.')[1]
	cmd = '''wget  --no-check-certificate -c "%s" -O %s/%s%s-%s '''%(file,outdir,file_name_1,thisday,file_name_2)
	file_down = outdir+'/'+file_name_1+thisday+'-'+file_name_2
	if not os.path.exists(file_down):
		os.system(cmd)
		filesize_down = os.path.getsize(file_down)
	else:
		print(file_down + ' exists')
		filesize_down = os.path.getsize(file_down)
		
	# 比较文件大小是否一致
	num = 0
	try:
		req = requests.get(file, allow_redirects=True, stream=True)
		filesize = int(req.headers['Content-length'])
		while filesize_down != filesize:
			if filesize_down < filesize: # 自动断点下载
				os.system(cmd)
				filesize_down = os.path.getsize(file_down)
			else:
				os.remove(file_down) # 删除文件，重新下载
				os.system(cmd)
				filesize_down = os.path.getsize(file_down)
			num = num+1
			if num > 3:
				break
	except Exception as e:
		if filesize_down > 464050933:
			return None
		else:
			while filesize_down < 464050933:
				os.remove(file_down)
				os.system(cmd)
				filesize_down = os.path.getsize(file_down)
				num = num+1
				if num > 3:
					break

def check_file_complete(file_list,outdir):
	'''
	purpose:检查所有文件是否下载完成
	args:
		file_list:需要下载的所有文件列表
	'''
	for file in file_list:
		down_load(file, outdir)

def main(thistime,htime,beforedays,thour,tdelta,outpath):
	'''主程序'''
	file_path = get_file_path(thistime,htime,thour,tdelta)
	before_file_path = befor_down_file(beforedays, htime,thistime)
	t_file_path = []
	t_file_path.extend(file_path);t_file_path.extend(before_file_path)
	print(len(t_file_path))
	outdir = outpath+'/'+thistime+'/'+htime+'/atmos'
	checkdir(outdir)
	outdir_list = [outdir]*len(t_file_path)
	with ThreadPoolExecutor(max_workers=24) as executer:
# 		all_work = {executer.submit(down_load, file,outdir): file for file in file_path}
		all_work = executer.map(down_load,t_file_path,outdir_list)
		for future in as_completed(all_work):
			work = all_work[future]
			try:
				data = future.result()
			except Exception as e:
				print('%r generated an exception: %s' % (work, e))
			else:
				print(data,future)
		# check_file_complete(t_file_path,outdir)

def main_process(thistime,htime,beforedays,thour,tdelta,outpath):
	'''多进程'''
	file_path = get_file_path(thistime,htime,thour,tdelta)
	before_file_path = befor_down_file(beforedays, htime,thistime)
	t_file_path = []
	t_file_path.extend(file_path);t_file_path.extend(before_file_path)
	print(len(t_file_path))
	
	outdir = outpath+'/'+thistime+'/'+htime+'/atmos'
	checkdir(outdir)
	with ProcessPoolExecutor(max_workers=52) as executer:
		all_work = {executer.submit(down_load, file,outdir): file for file in t_file_path}
		for future in as_completed(all_work):
			work = all_work[future]
			try:
				data = future.result()
			except Exception as e:
				print('%r generated an exception: %s' % (work, e))
			else:
				print(data,future)


if __name__ == '__main__':
	thistime = datetime.now().strftime('%Y%m%d')
	#thistime = '20220927'
	htime = '00'
	beforedays = 4
	thour = 216
	tdelta = 6
	outpath = '/disk_tiger/Sunchao'
	main(thistime,htime,beforedays,thour,tdelta,outpath)
	

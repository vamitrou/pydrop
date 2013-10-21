#!/usr/bin/python 

import os
import time
import json
import boto
import sys
import getopt
import yaml
import time 

from datetime import datetime


AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''


_local_path=None 
_bucket_name=None 
_conn=None
_bucket=None 


def sync_local_tree(folder, last_server_sync, server_files, array=[]):
	contents = os.listdir(folder)
	key = folder  #folder[len(_local_path):]
	
	for item in contents:
		full_filename = os.path.join(folder, item)
		if os.path.isfile(full_filename):
			last_modified = os.stat(full_filename).st_mtime
			file = os.path.normpath( key + os.sep  + os.path.basename(item) )
			if file[0] == '.':
				continue

			if last_server_sync < last_modified:
				array.append(file)
				print '[remote] add: ' + file 
				upload_file(file)

			elif server_files.get(file, None) == None:
				print '[local] delete: ' + file
				print 'Deleting local file'
				os.remove(full_filename)

			elif server_files[file] > last_modified:
				print 'Server version: %f, last modified: %s' % (server_files[file], last_modified)
				print '[local] update: ' + file
				download_file(file) 

			server_files.pop(file, None)
		else:
			sync_local_tree(full_filename, last_server_sync, server_files, array=array)
	
	return array


def fetch_orphan_remotes(new_server_files, last_modification=0):
	for file in new_server_files.keys():
		if new_server_files[file] > last_modification:
			print '[local] add: ' + file
			download_file(file)
		else:
			print '[Remote] delete: ' + file 
			delete_remote_file(file)


def get_remote_tree():
	conn = get_conn()
	bucket = get_bucket()
	files = {}
	for key in bucket.list():
		if key.name == '.server.conf':
			continue

		epoch = (datetime.strptime(key.last_modified[:-1], '%Y-%m-%dT%H:%M:%S.%f') 
							- datetime(1970, 1, 1)).total_seconds()
		files[_local_path + key.name] = epoch 
	print 'Remote files count: %d' % len(files) 
	return files 
	

def delete_remote_file(file):
	s3 = get_conn()
	bucket = get_bucket()
	bucket.get_key(file[len(_local_path):]).delete()
	print 'File deleted.'


def upload_file(file):
	print 'Uploading file: ' + file
	conn = get_conn()
	bucket = get_bucket()
	key = bucket.new_key(file[len(_local_path):])
	key.set_contents_from_filename(file)
	print 'Upload completed.'


def download_file(file):
	conn = get_conn()
	bucket = get_bucket()
	test = file[len(_local_path):]
	print 'Getting ' + test 
	file_key = bucket.get_key(file[len(_local_path):])
	if file_key is None:
		print file + ' does not exist.'
		return
	
	print 'Downloading ' + file
	path = os.path.join(_local_path, file)
	if not os.path.exists(os.path.dirname(path)):
		os.makedirs(os.path.dirname(path))

	file_key.get_contents_to_filename(path)
	print 'File downloaded.'


def get_file_as_string(file):
	conn = get_conn()
	bucket = get_bucket()
	file_key = bucket.get_key(file)
	if file_key is None:
		print file + ' does not exist'
		return ''
	
	return file_key.get_contents_as_string()


def get_conn():
	global _conn 
	if not _conn:
		_conn = boto.connect_s3(AWS_ACCESS_KEY_ID, 
					AWS_SECRET_ACCESS_KEY)	
	return _conn 


def get_bucket():
	global _bucket 
	if not _bucket:
		s3 = get_conn()
		_bucket = s3.get_bucket(_bucket_name)
	
	return _bucket


def write_local_config(bucket, folder, last_modify=0):
	conf = {'bucket_name': bucket}
	with open(os.path.expanduser('~/.pydrop.yaml'), 'w') as outfile:
    		outfile.write(yaml.dump(
			{ 'bucket_name': bucket, 
				'local_folder': folder, 
				'last_modify': last_modify },
			default_flow_style=False
		))


def load_local_config():
	global _local_path
	global _bucket_name 
	with open(os.path.expanduser('~/.pydrop.yaml'), 'r') as f:
		conf = yaml.load(f.read())
		_local_path = conf['local_folder']
		_bucket_name = conf['bucket_name']
	#print 'Config loaded: ' + _bucket_name + ' => ' + _local_path
	return conf


def get_remote_sync_date():
	conf = get_file_as_string('.server.conf')
	return yaml.load(conf)['last_sync']


def init_bucket(bucket, local_path):	
	write_local_config(bucket, local_path)
	
	s3 = get_conn()
	bucket = s3.create_bucket(bucket)  # bucket names must be unique
	
	server_conf = {"last_sync": time.time() }
	key = bucket.new_key('.server.conf')
	key.set_contents_from_string(yaml.dump(server_conf, default_flow_style=False))
	# key.set_acl('public-read')	


def refresh_server_date(epoch):
	s3 = get_conn()
	bucket = get_bucket()
	server_conf = {"last_sync": epoch }
	key = bucket.get_key('.server.conf')
	key.set_contents_from_string(yaml.dump(server_conf, default_flow_style=False))


def print_usage():
      	print 'usage: ' + __file__  + '  [--init --bucket-name <bucket_name> --local-folder <local_folder>]'


def main(argv):
	try:
		opts, args = getopt.getopt(argv,"h",["init", "bucket-name=", "local-folder="])
	except getopt.GetoptError, msg:
		print_usage()
		print msg
      		sys.exit(2)

	init = False 
	bucket_name = None
	local_folder = None
	for opt, arg in opts:
      		if opt == '-h':
         		print_usage()	
         		sys.exit()

		elif opt == '--init':
			init = True 

		elif opt == "--bucket-name":
			bucket_name = arg 
			# init_bucket(arg)

		elif opt == '--local-folder':
			local_folder = arg 
	
	if init:
		if not bucket_name or not local_folder:
			print_usage()
			sys.exit(2)
		
		path = os.path.normcase(os.path.expanduser(local_folder)) + os.sep
		if os.path.exists(path):
			print 'Local folder already exists'
			sys.exit(2)

		os.makedirs(path)
		print 'Local folder created.'
		
		try:
			init_bucket(bucket_name, path)
			print 'Bucket created'
		except Exception, msg:
			print 'Bucket already exists. Downloading files from S3.'
			fetch_orphan_remotes(get_remote_tree())
			# os.removedirs(path)
			# print 'Local folder removed'
			# print msg
			# sys.exit(2)
	
	else:
		if bucket_name or local_folder:
			print_usage()
			exit(2)
				
	i=0	
	while True:		
		print i
		conf = load_local_config()
		last_sever_sync = get_remote_sync_date()
		files = get_remote_tree()
		sync_local_tree(conf['local_folder'], last_sever_sync, files)
		fetch_orphan_remotes(files, conf['last_modify'])

		last_sync = time.time()
		write_local_config(_bucket_name, _local_path, last_sync)
		refresh_server_date(last_sync)
		#sys.exit(2)
		i += 1


if __name__ == '__main__':
	main(sys.argv[1:])

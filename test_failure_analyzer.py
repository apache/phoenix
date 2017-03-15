#!/usr/bin/python

import xml.etree.ElementTree as ET
import sys
import os
import csv
reload(sys)
sys.setdefaultencoding('utf-8')

fieldnames = ['classname', 'testcase', 'error_message','type','info'];
with open('calcite_failed_tests.csv', 'w') as csvfile:
  writer = csv.DictWriter(csvfile, fieldnames,dialect='excel',delimiter=',');
  writer.writeheader();
  target_dir = os.path.join(os.getcwd(),'phoenix-core/target');
  list_of_dirs = os.listdir(target_dir);
  for dir in list_of_dirs:
    if dir == "failsafe-reports" or dir == "surefire-reports":
      reports_dir = os.path.join(os.getcwd(),os.path.join('phoenix-core/target',dir));
      list_of_files = os.listdir(reports_dir);
      for each_file in list_of_files:
        if each_file.startswith('TEST-'):
          tree = ET.parse(os.path.join(reports_dir,each_file));
          root = tree.getroot()
          for elem in root.iter("testcase"):
            for child in elem.getchildren():
	      if child.tag == "failure" or child.tag == "error":
	        classname = elem.get('classname');
                testcase = elem.get('name');
                error_message = child.get('message');
                type = child.get('type');
                info = child.text;
	        writer.writerow({'classname':classname,'testcase':testcase,'error_message':error_message,'type':type,'info':info});
csvfile.close();

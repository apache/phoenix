#!/usr/bin/python

import xml.etree.ElementTree as ET
import sys
import os
import csv
import operator

reload(sys)
sys.setdefaultencoding('utf-8')

fieldnames = ['classname', 'testcase', 'error_message','type','info'];
singlelinecsvfile = open("single_line_calcite_failed_tests.csv", "w");
failed_tests = []
with open('calcite_failed_tests.csv', 'w') as csvfile:
  writer = csv.writer(csvfile);
  writer.writerow(fieldnames);
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
	      test_case_details = []
	      if child.tag == "failure" or child.tag == "error":
	        classname = elem.get('classname');
                testcase = elem.get('name');
                error_message = child.get('message');
                type = child.get('type');
                info = child.text;
		test_case_details.append(classname);
		test_case_details.append(testcase);
		test_case_details.append(error_message);
		test_case_details.append(type);
		test_case_details.append(info);
		failed_tests.append(test_case_details);
		singlelinecsvfile.write(classname+',');
		singlelinecsvfile.write(testcase+',');
		singlelinecsvfile.write(type.replace('\n',"\r")+',');
		singlelinecsvfile.write(info.replace('\n',"\r"));
		singlelinecsvfile.write("\b")
  sortedlist = sorted(failed_tests, key=operator.itemgetter(2))
  writer.writerows(sortedlist);
csvfile.close();
singlelinecsvfile.close();

'''
Reads .log-files and outputs summary of counters as a .json-file
'''
import sys
import os
import json


def main(log_file):
  read_error_dict(STRING_FILE, dict_G4_errors)

  dict_total = []

  #global LOG_STRING
  #global STRING_FILE

  FILE_OK = get_log_string(log_file)
  if not FILE_OK:
    print 'WARNING: Problems in reading ', log_file
    return

  reversed_keys = sorted(dict_G4_errors.keys(), reverse=True)

  for error_string in reversed_keys:
    dict_count_dump_error_string = dict()
    dict_test = dict()
    ctest = LOG_STRING.count(error_string)
    test = LOG_STRING.find(error_string)
    array = []
    for i in range(0, ctest):

      start = test
      test = LOG_STRING.find(error_string, start)
      already_found = False
      for error in reversed_keys:
        if error == error_string:
          break
        checke = LOG_STRING[test:test + 100].find(error)
        if checke != -1:
          already_found = True
          test = test + len(error)
          break
      if already_found:
        continue

      if test != -1:
        eventnr = ''
        runnr = ''

        eventnr_point = LOG_STRING.rfind('INFO Evt', test - 5000, test)
        if eventnr_point != -1:
          eventnr = 'Evt ' + LOG_STRING[eventnr_point:test].split('INFO Evt')[1].strip().split(',')[0]
          runnr = LOG_STRING[eventnr_point:].split('INFO Evt')[1].strip().split(',')[1]

        if error_string.find('G4') != -1:
          check = LOG_STRING[test:test + 250].find('***')
          if check != -1:
            error_base = LOG_STRING[test:test + 250].split('***')[0]
            dict_count_dump_error_string[i] = eventnr + "  " + runnr + "  -->" + error_base

            array.append(dict())
            array[-1]['eventnr'] = eventnr
            array[-1]['runnr'] = runnr

            length_dump = len(error_base)
            test = test + length_dump
        else:
          error_base = LOG_STRING[test:test + 250].split('\n')[0]
          dict_count_dump_error_string[i] = eventnr + "  " + runnr + "  -->" + error_base

          array.append(dict())
          array[-1]['eventnr'] = eventnr
          array[-1]['runnr'] = runnr

          length_dump = len(error_base)
          test = test + length_dump

        if array[-1] is not None:
          dict_test[error_string] = dict()
          dict_test[error_string] = array

    if dict_test != {}:
      dict_total.append(dict_test)
    dict_G4_errors_count[error_string] = dict_count_dump_error_string
  create_json_table(dict_total, "errors.json")
  create_HTML_table(dict_G4_errors_count, "errors.html")

################################################

# def create_json_table(dict_total, name):

#   # Due to issues in the mapping of the ES DB, this mapping
#   (which is more clear than the one below) couldn't be used.
#   # I have still saved the function here.

#   ids = {}
#   ids['JobID'] = JOB_ID
#   ids['ProductionID'] = PROD_ID
#   ids['TransformationID'] = TRANS_ID

#   errors = []

#   with open(name, 'w') as output:
#     for error in dict_total:
#       newrow = {}
#       for key, value in error.items():
#         newrow['Error type'] = key
#         newrow['Counter'] = len(value)
#         newrow['Events'] = value
#       errors.append(newrow)

#     errorDict = {'Errors' : errors}

#     result = {}
#     result['ID'] = ids
#     result['Errors'] = errors
#     result = {'Log_output' : result}

#     output.write(json.dumps(result, indent = 2))
#   return

#####################################################


def create_json_table(dict_total, name):
  result = {}
  temp = {}
  ids = {}
  ids['JobID'] = JOB_ID
  ids['ProductionID'] = PROD_ID
  ids['TransformationID'] = TRANS_ID

  with open(name, 'w') as output:
    for error in dict_total:
      for key, value in error.items():
        temp[key] = len(value)
    temp['ID'] = ids
    result['Errors'] = temp
    print result
    output.write(json.dumps(result, indent=2))
  return

#####################################################


def create_HTML_table(dict_G4_errors_count, name):
  f = open(name, 'w')
  f.write("<HTML>\n")

  f.write("<table border=1 bordercolor=#000000 width=100% bgcolor=#BCCDFE>")
  f.write("<tr>")
  f.write("<td>ERROR TYPE</td>")
  f.write("<td>COUNTER</td>")
  f.write("<td>DUMP OF ERROR MESSAGES</td>")
  f.write("</tr>")

  orderedKeys = sorted(dict_G4_errors_count.keys())
  for errString in orderedKeys:
    if dict_G4_errors_count[errString] != {}:
      f.write("<tr>")
      f.write("<td>" + errString + "</td>")
      f.write("<td>" + str(len(dict_G4_errors_count[errString].keys())) + "</td>")
      f.write("<td>")
      f.write("<lu>")
      for y in dict_G4_errors_count[errString].keys():
        f.write("<li>")
        f.write(" " + dict_G4_errors_count[errString][y] + " ")
      f.write("</lu>")
      f.write("</td>")
      f.write("</tr>")

  f.write("</table>")

  return

#######################################################


def read_error_dict(string_file, dict_name):
  'Reads errors in a stringfile and puts them in dict_name'
  file_lines = get_lines(string_file)
  for line in file_lines:
    error_string = line.split(',')[0]
    description = line.split(',')[1]
    dict_name[error_string] = description
  return

################################################


def get_lines(string_file):
  'Reads lines from string file'
  print '>>> Processed STRINGFILE -> ', string_file
  with open(string_file, 'r') as f:
    lines = f.readlines()
  return lines

################################################


def get_log_string(log_file):
  global LOG_STRING, FILE_OK
  FILE_OK = False
  print 'Attempting to open %s' % log_file
  if not os.path.exists(log_file):
    print '%s could not be found' % log_file
    return
  if os.stat(log_file)[6] == 0:
    print '%s is empty' % log_file
    return
  with open(log_file, 'r') as f:
    LOG_STRING = f.read()
  FILE_OK = True
  return FILE_OK

################################################


def pick_string_file(project, version):
  #source_dir = commands.getoutput('echo $PWD') + '/errstrings'
  source_dir = os.getcwd()
  file_string = project + '_' + version + '_errors.txt'
  STRING_FILE = os.path.join(source_dir, os.path.basename(file_string))
  if not os.path.exists(STRING_FILE):
    print 'string file %s does not exist, attempting to take the most recent file ...' % STRING_FILE
    file_list = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.find(project) != -1]
    if len(file_list) != 0:
      file_list.sort()
      STRING_FILE = file_list[len(file_list) - 1]
    else:
      print 'WARNING: no string files for this project'
      return None

  return STRING_FILE

################################################


global LOG_STRING
global STRING_FILE
global FILE_OK

LOG_FILE = sys.argv[1]
PROJECT = sys.argv[2]
VERSION = sys.argv[3]

global JOB_ID
global PROD_ID
global TRANS_ID
###
global COUNT
###

JOB_ID = sys.argv[4]
PROD_ID = sys.argv[5]
TRANS_ID = sys.argv[6]
###
COUNT = sys.argv[7]
###

LOG_STRING = ''
FILE_OK = ''
dict_G4_errors = dict()
dict_G4_errors_count = dict()
STRING_FILE = pick_string_file(PROJECT, VERSION)

if STRING_FILE is not None:
  if os.stat(STRING_FILE)[6] != 0:
    main(LOG_FILE)
  else:
    print 'WARNING: STRINGFILE %s is empty' % STRING_FILE

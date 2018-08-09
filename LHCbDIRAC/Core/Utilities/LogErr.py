'''
Reads .log-files and outputs summary of counters as a .json-file
'''
import os
import json
from DIRAC import gLogger, S_ERROR, S_OK


def readLogFile(log_file, project, version, jobID, prodID, wmsID):

  fileOK = False
  logString = ''
  stringFile = ''
  stringFile = pick_string_file(project, version, stringFile)
  dict_total = []
  dict_G4_errors = dict()
  dict_G4_errors_count = dict()

  if stringFile is None or os.stat(stringFile)[6] == 0:
    gLogger.warn('WARNING: STRINGFILE %s is empty' % stringFile)

  read_error_dict(stringFile, dict_G4_errors)

  fileOK = get_log_string(log_file, logString, fileOK)
  if not fileOK:
    gLogger.warn('WARNING: Problems in reading %s' % log_file)
    return S_ERROR()

  reversed_keys = sorted(dict_G4_errors.keys(), reverse=True)

  for error_string in reversed_keys:
    dict_count_dump_error_string = dict()
    dict_test = dict()
    ctest = logString.count(error_string)
    test = logString.find(error_string)
    array = []
    for i in range(0, ctest):

      start = test
      test = logString.find(error_string, start)
      already_found = False
      for error in reversed_keys:
        if error == error_string:
          break
        checke = logString[test:test + 100].find(error)
        if checke != -1:
          already_found = True
          test = test + len(error)
          break
      if already_found:
        continue

      if test != -1:
        eventnr = ''
        runnr = ''

        eventnr_point = logString.rfind('INFO Evt', test - 5000, test)
        if eventnr_point != -1:
          eventnr = 'Evt ' + logString[eventnr_point:test].split('INFO Evt')[1].strip().split(',')[0]
          runnr = logString[eventnr_point:].split('INFO Evt')[1].strip().split(',')[1]

        if error_string.find('G4') != -1:
          check = logString[test:test + 250].find('***')
          if check != -1:
            error_base = logString[test:test + 250].split('***')[0]
            dict_count_dump_error_string[i] = eventnr + "  " + runnr + "  -->" + error_base

            array.append(dict())
            array[-1]['eventnr'] = eventnr
            array[-1]['runnr'] = runnr

            length_dump = len(error_base)
            test = test + length_dump
        else:
          error_base = logString[test:test + 250].split('\n')[0]
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
  create_json_table(dict_total, "errors.json", jobID, prodID, wmsID)
  create_HTML_table(dict_G4_errors_count, "errors.html")

################################################

#   # Due to issues in the mapping of the ES DB, this mapping
#   (which is more clear than the one below) couldn't be used.
#   # I have still saved the function here.

# def create_json_table(dict_total, name):

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


def create_json_table(dict_total, name, jobID, prodID, wmsID):

  result = {}
  temp = {}
  ids = {}
  ids['JobID'] = jobID
  ids['ProductionID'] = prodID
  ids['wmsID'] = wmsID

  with open(name, 'w') as output:
    for error in dict_total:
      for key, value in error.items():
        temp[key] = len(value)
    temp['ID'] = ids
    result['Errors'] = temp
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
  gLogger.notice('>>> Processed STRINGFILE -> ', string_file)
  with open(string_file, 'r') as f:
    lines = f.readlines()
  return lines

################################################


def get_log_string(log_file, logString, fileOK):
  gLogger.notice('Attempting to open %s' % log_file)
  if not os.path.exists(log_file):
    gLogger.error('%s could not be found' % log_file)
    return S_ERROR()
  if os.stat(log_file)[6] == 0:
    gLogger.error('%s is empty' % log_file)
    return S_ERROR()
  with open(log_file, 'r') as f:
    logString = f.read()
  fileOK = True
  gLogger.notice("Successfully read %s" % log_file)
  return fileOK

################################################


def pick_string_file(project, version, stringFile):
  # source_dir = commands.getoutput('echo $PWD') + '/errstrings'
  source_dir = os.getcwd()
  file_string = project + '_' + version + '_errors.txt'
  stringFile = os.path.join(source_dir, os.path.basename(file_string))
  if not os.path.exists(stringFile):
    gLogger.notice('string file %s does not exist, attempting to take the most recent file ...' % stringFile)
    file_list = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if f.find(project) != -1]
    if len(file_list) != 0:
      file_list.sort()
      stringFile = file_list[len(file_list) - 1]
    else:
      gLogger.warn('WARNING: no string files for this project')
      return None

  return stringFile

################################################

# This is a relic from the previous version of the file, I'll keep it for the while

# global LOG_STRING
# global STRING_FILE
# global FILE_OK

# LOG_FILE = sys.argv[1]
# PROJECT = sys.argv[2]
# VERSION = sys.argv[3]

# global JOB_ID
# global PROD_ID
# global TRANS_ID

# JOB_ID = sys.argv[4]
# PROD_ID = sys.argv[5]
# TRANS_ID = sys.argv[6]

# #LOG_STRING = ''
# #FILE_OK = ''
# dict_G4_errors = dict()
# dict_G4_errors_count = dict()
# STRING_FILE = pick_string_file(PROJECT, VERSION)

# if STRING_FILE is not None:
#   if os.stat(STRING_FILE)[6] != 0:
#     main(LOG_FILE)
#   else:
#     print 'WARNING: STRINGFILE %s is empty' % STRING_FILE

# The file is run as follows:
# readLogFile('Example.log', 'project', 'version', 'jobID', 'prodID', 'wmsID')

#!/usr/bin/python3

import os, sys

try:
    import argparse
except ModuleNotFoundError:
    print('Error: Please install Python argparse module', file=sys.stderr)
    sys.exit(1)

try:
    import pandas as pd
except ModuleNotFoundError:
    print('Error: Please install Python pandas module', file=sys.stderr)
    sys.exit(1)

try:
    import omero.clients
    from omero.gateway import BlitzGateway
except:
    print('Error: Please install Python Omero module', file=sys.stderr)
    sys.exit(1)

my_parser = argparse.ArgumentParser()

my_parser.add_argument("-i", "--input", default=None, help="the file to be validated")
my_parser.add_argument("-s", "--separator", default='\t', help="the character the file columns are separated by")
my_parser.add_argument("-m", "--mode", default='import', help="type of validation needed (import or stitching)")
my_parser.add_argument("-u", "--user", default=None, help="omero username to log in with")
my_parser.add_argument("-p", "--password", default=None, help="omero password to log in with")

args = my_parser.parse_args()

if args.input is None:
  print('Error: Input file is not specified', file=sys.stderr)
  sys.exit(1)

if args.user is None:
  print('Error: Omero user is not specified', file=sys.stderr)
  sys.exit(1)

if args.password is None:
  print('Error: Omero password is not specified', file=sys.stderr)
  sys.exit(1)

if args.separator == '\t':
    print('Input separator is set to tab')
elif args.separator == ' ':
    print('Input separator is set to space')
else:
    print('Input separator is set to ' + str(args.separator))

if args.mode == 'import' or args.mode == 'stitching':
    pass
else:
    print('Error: Invalid mode selected. Please select import or stitching mode', file=sys.stderr)
    sys.exit(1)

## Checking file exists
try:
    input_file = pd.read_csv(args.input, sep=args.separator)
except FileNotFoundError:
    print('Error: File not found. Check path to file', file=sys.stderr)
    sys.exit(1)

input_columns = list(input_file.columns)

stripped_columns = []
for column in input_columns:
    ## Checking all mandatory columns are named
    if 'Unnamed' in column:
        index = input_columns.index(column)
        print('Error: Column number ' + str(index) + ' does not have a column name', file=sys.stderr)
        sys.exit(1)
    ## Removing Special 
    stripped = column.strip()
    stripped_columns.append(stripped)

if args.mode == 'import':
    expected_columns = ['filename', 'location', 'OMERO_SERVER', 'Project', 'OMERO_internal_group', 'OMERO_project', 'OMERO_DATASET', 'OMERO_internal_users']
    for column in stripped_columns:
        if column not in expected_columns:
            print('Error: column "' + column + '" is not an expected column name', file=sys.stderr)
            sys.exit(1)
    input_file.columns = stripped_columns
    ## Checking image file exists
    image_file = str(input_file['location'][0]) + '/' + str(input_file['filename'][0])
    image_exists = os.path.exists(image_file)
    if image_exists is False:
        print('Error: Image file does not exist', file=sys.stderr)
        sys.exit(1)
    ## Checking is columns are empty
    for column in stripped_columns:
        is_empty = bool(input_file[column].isnull().values.all())
        if is_empty is True:
            print('Error: column ' + column + ' is empty', file=sys.stderr)
            sys.exit(1)
    ## Checking User in Group
    conn = BlitzGateway(args.user, args.password, host="omero-srv2", secure=True)
    conn.connect()
    session = conn.c.getSession()
    admin_service = session.getAdminService()
    groupId = admin_service.lookupGroup(str(input_file['OMERO_internal_group'][0])).getId()
    users = admin_service.containedExperimenters(groupId.val)
    user_in_group = False
    user_list = []
    for user in users:
      user_list.append(user.omeName.val)
    if str(input_file['OMERO_internal_users'][0]) not in user_list:
      print('Error: Omero user is not in omero group', file=sys.stderr)
      conn.close()
      sys.exit(1)
    ## Checking Project Exists
    projects_df = pd.DataFrame(columns=["id","groupName"])
    for group in admin_service.lookupGroups():
      projects_df = projects_df.append({'id': group.getId().val, 'groupName': group.getName().val}, ignore_index=True)
    project_names = list(projects_df['groupName'])
    if str(input_file['Project'][0]) not in project_names:
      print('Error: project ' + str(input_file['Project'][0]) + ' does not exist', file=sys.stderr)
    conn.close()

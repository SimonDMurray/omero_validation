#!/usr/bin/python3

import os, sys, glob, warnings

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
    import openpyxl
except ModuleNotFoundError:
    print('Error: Please install Python openpyxl module', file=sys.stderr)
    sys.exit(1)

try:
    import omero.clients
    from omero.gateway import BlitzGateway
except:
    print('Error: Please install Python Omero module', file=sys.stderr)
    sys.exit(1)

warnings.filterwarnings("ignore")

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

#if args.mode == 'import':
#  ## Checking file exists
#  try:
#    input_file = pd.read_csv(args.input, sep=args.separator)
#  except FileNotFoundError:
#    print('Error: File not found. Check path to file', file=sys.stderr)
#    sys.exit(1)
#  input_columns = list(input_file.columns)
#  stripped_columns = []
#  for column in input_columns:
#    ## Checking all mandatory columns are named
#    if 'Unnamed' in column:
#      index = input_columns.index(column)
#      print('Error: Column number ' + str(index) + ' does not have a column name', file=sys.stderr)
#      sys.exit(1)
#    ## Removing Special 
#    stripped = column.strip()
#    stripped_columns.append(stripped)
#  expected_columns = ['filename', 'location', 'OMERO_SERVER', 'Project', 'OMERO_internal_group', 'OMERO_project', 'OMERO_DATASET', 'OMERO_internal_users']
#  for column in stripped_columns:
#    if column not in expected_columns:
#      print('Error: column "' + column + '" is not an expected column name', file=sys.stderr)
#      sys.exit(1)
#  input_file.columns = stripped_columns
#  ## Checking image file exists
#  image_file = str(input_file['location'][0]) + '/' + str(input_file['filename'][0])
#  image_exists = os.path.exists(image_file)
#  if image_exists is False:
#    print('Error: Image file does not exist', file=sys.stderr)
#    sys.exit(1)
#  ## Checking if columns are empty
#  for column in stripped_columns:
#    is_empty = bool(input_file[column].isnull().values.all())
#    if is_empty is True:
#      print('Error: column ' + column + ' is empty', file=sys.stderr)
#      sys.exit(1)
#  ## Iterating through rows for multiple submissions
#  for index, row in input_file.iterrows():
#    ## Checking User in Group
#    conn = BlitzGateway(args.user, args.password, host="omero-srv2", secure=True)
#    conn.connect()
#    session = conn.c.getSession()
#    admin_service = session.getAdminService()
#    groupId = admin_service.lookupGroup(str(input_file['OMERO_internal_group'][index])).getId()
#    users = admin_service.containedExperimenters(groupId.val)
#    user_in_group = False
#    user_list = []
#    for user in users:
#      user_list.append(user.omeName.val)
#    if str(input_file['OMERO_internal_users'][index]) not in user_list:
#      print('Error: Omero user ' + str(input_file['OMERO_internal_users'][index]) + ' is not in omero group', file=sys.stderr)
#      conn.close()
#      sys.exit(1)
#    ## Checking Project Exists
#    projects_df = pd.DataFrame(columns=["id","groupName"])
#    for group in admin_service.lookupGroups():
#      projects_df = projects_df.append({'id': group.getId().val, 'groupName': group.getName().val}, ignore_index=True)
#    project_names = list(projects_df['groupName'])
#    if str(input_file['Project'][index]) not in project_names:
#      print('Error: project ' + str(input_file['Project'][index]) + ' does not exist', file=sys.stderr)
#      conn.close()
#      sys.exit(1)
#    conn.close()

if args.mode == 'stitching':
  ## Checking file exists
  try:
    workbook = openpyxl.load_workbook(args.input)
    #input_file = pd.read_excel(args.input)
  except FileNotFoundError:
    print('Error: File not found. Check path to file', file=sys.stderr)
    sys.exit(1)
  ## Converting workbook to dataframe
  worksheet = workbook['Sheet1']
  worksheet_data = worksheet.values
  ## Get the first line in file as a header line
  worksheet_columns = next(worksheet_data)[0:]
  ## Create a DataFrame based on the second and subsequent lines of data
  input_file = pd.DataFrame(worksheet_data, columns=worksheet_columns)
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
  expected_columns = ['Researcher', 'Project', 'SlideID', 'Automated_PlateID', 'SlideN', 'Slide_barcode', 'Species',
                    'Tissue_1', 'Sample_1', 'Age_1', 'Genotype_1', 'Background_1', 'Tissue_2', 'Sample_2', 'Age_2',
                    'Genotype_2', 'Background_2', 'Tissue_3', 'Sample_3', 'Age_3', 'Genotype_3', 'Background_3',
                    'Tissue_4', 'Sample_4', 'Age_4', 'Genotype_4', 'Background_4', 'Technology', 'Image_cycle',
                    'Channel1', 'Target1', 'Channel2', 'Target2', 'Channel3', 'Target3', 'Channel4', 'Target4',
                    'Channel5', 'Target5', 'Channel6', 'Target6', 'Channel7', 'Target7', 'Post-stain', 'Date',
                    'Measurement', 'Low_mag_reference', 'Mag_Bin_Overlap', 'Sections', 'SectionN', 'z-planes',
                    'Notes_1', 'Notes_2', 'Export_location', 'Archive_location', 'Harmony_copy_deleted', 'Team_dir',
                    'Pipeline', 'Microscope', 'Stitching_Z', 'Stitching_ReferenceChannel', 'Registration_ReferenceCycle',
                    'Registration_ReferenceChannel', 'OMERO_project', 'OMERO_DATASET', 'OMERO_internal_group', 'OMERO_internal_users'] 
  for column in stripped_columns:
    if column not in expected_columns:
      print('Error: column "' + column + '" is not an expected column name', file=sys.stderr)
      sys.exit(1)
  input_file.columns = stripped_columns
  mandatory_columns = ['Researcher', 'Project', 'SlideID', 'Automated_PlateID', 'Tissue_1', 'Sample_1', 'Channel1', 'Target1', 
                      'Measurement', 'Mag_Bin_Overlap', 'Export_location', 'Stitching_Z', 'OMERO_internal_group', 'OMERO_internal_users']
  ## Iterating through rows for multiple submissions
  for index, row in input_file.iterrows():
    ## Checking if columns are empty
    for column in mandatory_columns:
      is_empty = input_file[column][index]
      if is_empty is None:
        if column == 'Stitching_Z':
          input_file[column][index] = 'max'
        elif column == 'SlideID':
          plate = input_file['Automated_PlateID'][index]
          if plate is None:
            print('Error: column ' + column + ' for row ' + str(index+1) + ' is empty and there is no Automated_PlateID value', file=sys.stderr)
            sys.exit(1)
          slide_empty = True
          plate_empty = False
        elif column == 'Automated_PlateID':
          slide = input_file['SlideID'][index]
          if slide is None:
            print('Error: column ' + column + ' for row ' + str(index+1) + ' is empty and there is no SlideID value', file=sys.stderr)
            sys.exit(1)
          slide_empty = False
          plate_empty = True
    ## Checking User in Group
    conn = BlitzGateway(args.user, args.password, host="omero-srv2", secure=True)
    conn.connect()
    session = conn.c.getSession()
    admin_service = session.getAdminService()
    groupId = admin_service.lookupGroup(str(input_file['OMERO_internal_group'][index])).getId()
    users = admin_service.containedExperimenters(groupId.val)
    user_in_group = False
    user_list = []
    for user in users:
      user_list.append(user.omeName.val)
    if str(input_file['OMERO_internal_users'][index]) not in user_list:
      print('Error: Omero user ' + str(input_file['OMERO_internal_users'][index]) + ' is not in omero group', file=sys.stderr)
      conn.close()
      sys.exit(1)
    ## Checking Project Exists
    projects_df = pd.DataFrame(columns=["id","groupName"])
    for group in admin_service.lookupGroups():
      projects_df = projects_df.append({'id': group.getId().val, 'groupName': group.getName().val}, ignore_index=True)
    project_names = list(projects_df['groupName'])
    if str(input_file['Project'][index]) not in project_names:
      print('Error: project ' + str(input_file['Project'][index]) + ' does not exist', file=sys.stderr)
      conn.close()
      sys.exit(1)
    conn.close()
    ## Check image file exists
    export = str(input_file['Export_location'][index])
    if 'Harmony' in export:
      basepath = '/nfs/team283_imaging/'
      if slide_empty is False:
        image_exists = glob(basepath + str(input_file['Project'][index]) + '/' + str(input_file['SlideID'][index]) + '__' + '*' + str(input_file['Measurement'][index]) 
      if plate_empty is False:
        image_exists = glob(basepath + str(input_file['Project'][index]) + '/' + str(input_file['Automated_PlateID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
#      #glob
#    elif 'x' in export:
#      basepath = '/nfs/team172_spatial_genomics/RNAscope/'
#    elif 'y' in export:
#      basepath = '/nfs/team172_spatial_genomics_imaging/'
#    date = str(input_file['Date'][index])[0:10].strip()
#    print(date)
#    file_loc = basepath + str(input_file['Project'][index]) + '/' + str(input_file['Automated_PlateID'][index]) + '__' + date 
#    ## Check the output file exists
#    /nfs/assembled_images/datasets/RV_MFI/RVMFI (slideid_sampleid(partly)_targets_measurement_F1T1_stitchingZ)

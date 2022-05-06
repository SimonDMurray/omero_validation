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


def argument_testing(args):
  """
  Checking arguments are valid
  """
  if args.input is None:
    print('Error: Input file is not specified', file=sys.stderr)
    sys.exit(1)
  if args.user is None:
    print('Error: Omero user is not specified', file=sys.stderr)
    sys.exit(1)
  if args.password is None:
    print('Error: Omero password is not specified', file=sys.stderr)
    sys.exit(1)
  if args.mode == 'import' or args.mode == 'stitching':
    pass
  else:
    print('Error: Invalid mode selected. Please select import or stitching mode', file=sys.stderr)
    sys.exit(1)

def reading_file(args):
  """
  Reading in file
  """
  try:
    workbook = openpyxl.load_workbook(args.input)
  except FileNotFoundError:
    print('Error: File not found. Check path to file', file=sys.stderr)
    sys.exit(1)
  worksheet = workbook['Sheet1']
  worksheet_data = worksheet.values
  worksheet_columns = next(worksheet_data)[0:]
  input_file = pd.DataFrame(worksheet_data, columns=worksheet_columns)
  return input_file

def checking_columns_exist(args, stripped_columns):
  """
  Checking all columns are named as expected
  """
  if args.mode == 'import':
    expected_columns = ['filename', 'location', 'OMERO_SERVER', 'Project', 'OMERO_project', 'OMERO_DATASET', 'OMERO_internal_users']
  elif args.mode == 'stitching':
    expected_columns = ['Researcher', 'Project', 'SlideID', 'Automated_PlateID', 'SlideN', 'Slide_barcode', 'Species',
                      'Tissue_1', 'Sample_1', 'Age_1', 'Genotype_1', 'Background_1', 'Tissue_2', 'Sample_2', 'Age_2',
                      'Genotype_2', 'Background_2', 'Tissue_3', 'Sample_3', 'Age_3', 'Genotype_3', 'Background_3',
                      'Tissue_4', 'Sample_4', 'Age_4', 'Genotype_4', 'Background_4', 'Technology', 'Image_cycle',
                      'Channel1', 'Target1', 'Channel2', 'Target2', 'Channel3', 'Target3', 'Channel4', 'Target4',
                      'Channel5', 'Target5', 'Channel6', 'Target6', 'Channel7', 'Target7', 'Post-stain', 'Date',
                      'Measurement', 'Low_mag_reference', 'Mag_Bin_Overlap', 'Sections', 'SectionN', 'z-planes',
                      'Notes_1', 'Notes_2', 'Export_location', 'Archive_location', 'Harmony_copy_deleted', 'Team_dir',
                      'Pipeline', 'Microscope', 'Stitching_Z', 'Stitching_ReferenceChannel', 'Registration_ReferenceCycle',
                      'Registration_ReferenceChannel', 'OMERO_project', 'OMERO_DATASET', 'OMERO_internal_group',
                      'OMERO_internal_users']
  for column in stripped_columns:
    if column not in expected_columns:
      print('Error: column "' + column + '" is not an expected column name', file=sys.stderr)
      print('Please visit https://cellgeni.readthedocs.io/en/latest/imaging.html#id1 for guidance on column names', file=sys.stderr)
      sys.exit(1)
  for column in expected_columns:
    if column not in stripped_columns:
      print('Error: column "' + column + '" is not present', file=sys.stderr)
      print('Please visit https://cellgeni.readthedocs.io/en/latest/imaging.html#id1 for guidance on column names', file=sys.stderr)
      sys.exit(1)
  return expected_columns

def sanitising_header(args, input_file):
  """
  Ensuring there are no special characters in header
  and that all column names are named
  Removes any blank columns or rows
  """
  input_file = input_file.dropna(axis='index', how = 'all')
  if args.mode == 'import':
    input_file = input_file.iloc[:, :7]
  if args.mode == 'stitching':
    input_file = input_file.iloc[:, :67]
  input_columns = list(input_file.columns)
  stripped_columns = []
  for column in input_columns:
    if 'Unnamed' in column:
      index = input_columns.index(column)
      print('Error: Column number ' + str(index) + ' does not have a column name', file=sys.stderr)
      sys.exit(1)
    stripped = column.strip()
    stripped_columns.append(stripped)
  expected_columns = checking_columns_exist(args, stripped_columns)
  input_file.columns = stripped_columns
  if args.mode == 'import':
    mandatory_columns = expected_columns
  elif args.mode == 'stitching':
    mandatory_columns = ['Researcher', 'Project', 'SlideID', 'Automated_PlateID', 'Tissue_1', 'Sample_1', 'Channel1', 'Target1', 
                        'Measurement', 'Mag_Bin_Overlap', 'Export_location', 'Stitching_Z', 'OMERO_internal_group', 'OMERO_internal_users']
  return input_file, mandatory_columns

def checking_empty_columns(input_file, index, mandatory_columns):
  """
  Checking all mandatory columns are not empty
  """
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
      else:
        print('Error: column ' + column + ' is empty', file=sys.stderr)
        sys.exit(1)
    elif is_empty is not None:
      if column == 'SlideID':
        slide_empty = False
      elif column == 'Automated_PlateID':
        plate_empty = False

def user_in_group(input_file, index, conn, admin_service):
  """
  Checking the submitted internal omero user
  is in the omero group
  """
  groupId = admin_service.lookupGroup(str(input_file['Project'][index])).getId()
  users = admin_service.containedExperimenters(groupId.val)
  user_in_group = False
  user_list = []
  for user in users:
    user_list.append(user.omeName.val)
  if str(input_file['OMERO_internal_users'][index]) not in user_list:
    print('Error: Omero user ' + str(input_file['OMERO_internal_users'][index]) + ' is not in omero group ' + str(input_file['Project'][index]), file=sys.stderr)
    conn.close()
    sys.exit(1)

def project_exists(input_file, index, conn, admin_service):
  """
  Checking Omero project exists
  """
  projects_df = pd.DataFrame(columns=["id","groupName"])
  for group in admin_service.lookupGroups():
    projects_df = projects_df.append({'id': group.getId().val, 'groupName': group.getName().val}, ignore_index=True)
  project_names = list(projects_df['groupName'])
  if str(input_file['Project'][index]) not in project_names:
    print('Error: project ' + str(input_file['Project'][index]) + ' does not exist', file=sys.stderr)
    conn.close()
    sys.exit(1)

def glob_image(path):
  """
  globbing path to image
  """
  image_exists = glob.glob(path)
  return image_exists

def checking_image_file(args, input_file, index):
  """
  Checking input image file exists
  """
  if args.mode == 'import':
    path = str(input_file['location'][index]) + '/' + str(input_file['filename'][index])
    image_exists = glob_image(path)
    if len(image_exists) == 0:
      print('Error: Cannot find image. Use a FARM path as shown on the docs.', file=sys.stderr)
      print('Please visit https://cellgeni.readthedocs.io/en/latest/imaging.html#id1 an example', file=sys.stderr)
      sys.exit(1)
    elif len(image_exists) > 1:
      print('Error: Multiple of the same image found with different names.', file=sys.stderr)
      sys.exit(1)
  if args.mode == 'Stitching':
    export = str(input_file['Export_location'][index])
    if 'Harmony' in export:
      path = '/nfs/team283_imaging/0HarmonyExports/' + str(input_file['Project'][index]) + '/' + str(input_file['SlideID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
      image_exists = glob_image(path)
      if len(image_exists) == 0:
        path = '/nfs/team283_imaging/0HarmonyExports/' + str(input_file['Project'][index]) + '/' + str(input_file['Automated_PlateID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
        image_exists = glob_image(path)
        if len(image_exists) == 0:
          print('Error: Cannot find image. Use a FARM path as shown on the docs.', file=sys.stderr)
          print('Please visit https://cellgeni.readthedocs.io/en/latest/imaging.html#id1 an example', file=sys.stderr)
          sys.exit(1)
      elif len(image_exists) > 1:
        print('Error: Multiple of the same image found with different names.', file=sys.stderr)
        sys.exit(1)
    else:
      path = '/nfs/team172_spatial_genomics/RNAscope/' + str(input_file['Project'][index]) + '/' + str(input_file['SlideID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
      image_exists = glob_image(path)
      if len(image_exists) == 0:
        path = '/nfs/team172_spatial_genomics/RNAscope/' + str(input_file['Project'][index]) + '/' + str(input_file['Automated_PlateID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
        image_exists = glob_image(path)
        if len(image_exists) == 0:
          path = '/nfs/team172_spatial_genomics_imaging/' + str(input_file['Project'][index]) + '/' + str(input_file['SlideID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
          image_exists = glob_image(path)
          if len(image_exists) == 0:
            path = '/nfs/team172_spatial_genomics_imaging/' + str(input_file['Project'][index]) + '/' + str(input_file['Automated_PlateID'][index]) + '__' + '*' + str(input_file['Measurement'][index])
            if len(image_exists) == 0:
              print('Error: Cannot find image. Use a FARM path as shown on the docs.', file=sys.stderr)
              print('Please visit https://cellgeni.readthedocs.io/en/latest/imaging.html#id1 an example', file=sys.stderr)
              sys.exit(1)
      elif len(image_exists) > 1:
        print('Error: Multiple of the same image found with different names.', file=sys.stderr)
        sys.exit(1)

def check_assembled_images(input_file, index):
  """
  Checking if image is already assembled
  """
  path = '/nfs/assembled_images/datasets/' + str(input_file['Project'][index]) + '/' + str(input_file['Project'][index]) + '/' + str(input_file['SlideID'][index]) + '_' + str(input_file['Sample_1'][index])[0:7] + '*' + 'Meas' + str(input_file['Measurement'][index]) + '*' + str(input_file['Stitching_Z'][index]) + '.ome.tif'
  image_exists = glob_image(path)
  if len(image_exists) == 0:
    path = '/nfs/assembled_images/datasets/' + str(input_file['Project'][index]) + '/' + str(input_file['Project'][index]) + '/' + str(input_file['Slide_barcode'][index]) + '_' + str(input_file['Sample_1'][index])[0:7] + '*' + 'Meas' + str(input_file['Measurement'][index]) + '*' + str(input_file['Stitching_Z'][index]) + '.ome.tif'
    image_exists = glob_image(path)
    if len(image_exists) == 1:
      print('Image already assembled. No need to run pipeline.', file=sys.stderr)
      sys.exit(1)

def main():
  """
  Main function.
  Contains argument parsing.
  """
  my_parser = argparse.ArgumentParser()
  my_parser.add_argument("-i", "--input", default=None, help="the file to be validated")
  my_parser.add_argument("-m", "--mode", default='import', help="type of validation needed (import or stitching)")
  my_parser.add_argument("-u", "--user", default=None, help="omero username to log in with")
  my_parser.add_argument("-p", "--password", default=None, help="omero password to log in with")
  args = my_parser.parse_args()
  argument_testing(args)
  input_file = reading_file(args)
  input_file, mandatory_columns = sanitising_header(args, input_file)
  ## Iterating through rows for multiple submissions
  for index, row in input_file.iterrows():
    checking_empty_columns(input_file, index, mandatory_columns)
    conn = BlitzGateway(args.user, args.password, host="omero-srv2", secure=True)
    conn.connect()
    session = conn.c.getSession()
    admin_service = session.getAdminService()
    user_in_group(input_file, index, conn, admin_service)
    project_exists(input_file, index, conn, admin_service)
    conn.close()
    checking_image_file(args, input_file, index)
    if args.mode == 'stitching':
      check_assembled_images(input_file, index)
  output = str(args.mode) + '.tsv'
  input_file.to_csv(output, sep = '\t', index = False)

main()

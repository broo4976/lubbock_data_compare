"""
table_diff_compare.py
Brooke Reams, breams@esri.com
Esri - Database Services
Apr. 20, 2015
- Runs Frequency to find anomolies in unique identifiers.
- Finds the difference in unique identifiers between input tables and writes the results to two new tables.
- Compares user-selected fields between input tables and writes records of compare table to new table where
field values are not identical between tables.

Updates:

"""


import arcpy, os, sys, traceback##, datetime

##start_time = datetime.datetime.now()

# Get user-defined parameters
input_tbl = arcpy.GetParameterAsText(0) # Table View, required
comp_tbl = arcpy.GetParameterAsText(1) # Table View, required
common_flds = arcpy.GetParameterAsText(2) # Field, multi-value, required
out_gdb = arcpy.GetParameterAsText(3) # Workspace, filtered to file gdb, required
run_freq = arcpy.GetParameterAsText(4) # Bool, optional


# Hard coded variables
UNIQUEID_FLD = "BILLING_UNIQUEID"
MISSINGINPUT = "MISSING_INPUT_UNIQUEID"
MISSINGCOMPARE = "MISSING_COMPARE_UNIQUEID"
COMPARE = "COMPARE_OUTPUT"


# Field type mappings (return value from field type proeprty and field type to pass to Add Field GP tool)
fld_type_mappings = {"Blob": "BLOB", "Date": "DATE", "Double": "DOUBLE", "Geometry": "GEOMETRY",
                     "Guid": "GUID", "Integer": "LONG", "Raster": "RASTER", "Single": "FLOAT",
                     "SmallInteger": "SHORT", "String": "TEXT"}


# Overwrite output
arcpy.env.overwriteOutput = 1

try:
    # ********************************** FREQUENCY ********************************** #
    # Check if user checked on option to create frequency tables
    if run_freq == "true":
        # Run Frequency on GP tool to find duplicates unique IDs in tables
        out_freq_input_tbl = os.path.join(out_gdb, os.path.basename(input_tbl).replace(".", "_") + "_FREQUENCY")
        out_freq_comp_tbl = os.path.join(out_gdb, os.path.basename(comp_tbl).replace(".", "_") + "_FREQUENCY")
        arcpy.AddMessage("Creating " + os.path.basename(out_freq_input_tbl + " table"))
        arcpy.Frequency_analysis(input_tbl, out_freq_input_tbl, [UNIQUEID_FLD])
        arcpy.AddMessage("Creating " + os.path.basename(out_freq_comp_tbl + " table"))
        arcpy.Frequency_analysis(comp_tbl, out_freq_comp_tbl, [UNIQUEID_FLD])
        


    # ********************************** DIFF ********************************** #
    arcpy.AddMessage("Finding diffs between input and compare tables")
    # Open search cursor on input table, store all unique IDs in set
    input_set = set()
    with arcpy.da.SearchCursor(input_tbl, [UNIQUEID_FLD]) as cur:
        for row in cur:
            input_set.add(str(row[0]))

    # Open search cursor on compare table, store all unique IDs in set
    comp_set = set()
    with arcpy.da.SearchCursor(comp_tbl, [UNIQUEID_FLD]) as cur:
        for row in cur:
            comp_set.add(str(row[0]))

    # Find diff between compare set and input set
    missing_input_set = comp_set - input_set
    # Find diff between input set and compare set
    missing_comp_set = input_set - comp_set

    # If diff, create table and write out values
    if missing_input_set:
        arcpy.AddMessage("Writing {0:,g} records to {1} table".format(len(missing_input_set), MISSINGINPUT))
        # Create table to store unique IDs missing from input table
        missing_input_tbl = arcpy.CreateTable_management(out_gdb, MISSINGINPUT).getOutput(0)
        # Add field to store unique IDs
        arcpy.AddField_management(missing_input_tbl, UNIQUEID_FLD, "TEXT", field_length=50)
        # Insert values into table
        cur = arcpy.da.InsertCursor(missing_input_tbl, [UNIQUEID_FLD])
        for id in missing_input_set:
            row = (id,)
            cur.insertRow(row)
    else:
        arcpy.AddMessage("No unique values to write to " + MISSINGINPUT + " table")

    if missing_comp_set:
        arcpy.AddMessage("Writing {0:,g} records to {1} table".format(len(missing_comp_set), MISSINGCOMPARE))
        # Create table to store unique IDs missing from input table
        missing_comp_tbl = arcpy.CreateTable_management(out_gdb, MISSINGCOMPARE).getOutput(0)
        # Add field to store unique IDs
        arcpy.AddField_management(missing_comp_tbl, UNIQUEID_FLD, "TEXT", field_length=50)
        # Insert values into table
        cur = arcpy.da.InsertCursor(missing_comp_tbl, [UNIQUEID_FLD])
        for id in missing_comp_set:
            row = (id,)
            cur.insertRow(row)
    else:
        arcpy.AddMessage("No unique values to write to " + MISSINGCOMPARE + " table")



    # ********************************** COMPARE ********************************** #
    arcpy.AddMessage("Comparing input field values between input and compare tables")
    # Get set of unique IDs common in both tables
    common_set = input_set.intersection(comp_set)
    # Split field input parameter into list
    flds_list = common_flds.split(";")

    # Insert unique ID fld to front of list of fields
    all_flds = flds_list[:] # Use splicing so new list does not point to same list object
    all_flds.insert(0, UNIQUEID_FLD)
    # Initialize list to store all records where the common fields don't match
    notcommon_list = []
    # Write where clause to retrieve unique IDs common to both tables
    where = UNIQUEID_FLD + " IN " + str(tuple(common_set)) # SQL Server bug: https://connect.microsoft.com/SQLServer/feedback/details/521943/the-query-processor-ran-out-of-internal-resources-and-could-not-produce-a-query-plan-with-where-in-and-several-thousand-values
##    where = UNIQUEID_FLD + " = '"
##    where = where + ("' OR " + UNIQUEID_FLD + " = '").join(common_set) + "'" # Workaround for above bug if running on SQL Server, but this query is *significantly* slower
    # Loop through input table to retrieve field values and store in dictionary
    input_dict = {}
    with arcpy.da.SearchCursor(input_tbl, all_flds, where) as cur_input:
        for row_input in cur_input:
            input_dict[row_input[0]] = row_input[1:]

    # Loop through compare table to retrieve field values and store in dictionary
    comp_dict = {}
    with arcpy.da.SearchCursor(comp_tbl, all_flds, where) as cur_comp:
        for row_comp in cur_comp:
            comp_dict[row_comp[0]] = row_comp[1:]


    # Find differences in dictionaries
    out_rows = []
    for k, v in comp_dict.items():
        if v != input_dict[k]:
            l = list(v)
            l.insert(0, k)
            out_rows.append(l)


    # If list of rows has values, create output table
    if out_rows:
        # Create compare table
        arcpy.AddMessage("Writing {0:,g} records to {1} table".format(len(out_rows), COMPARE))
        comp_out_tbl = arcpy.CreateTable_management(out_gdb, COMPARE).getOutput(0)
        # Add fields
        for fld in all_flds:
            # Find field type
            cur_fld = arcpy.ListFields(input_tbl, fld)
            fld_type = fld_type_mappings[cur_fld[0].type]
            # If field is of type string, get the length to use when adding the field
            if fld_type == "String":
                fld_length = cur_fld[0].length
            else:
                fld_length = None
            arcpy.AddField_management(comp_out_tbl, fld, fld_type, field_length=fld_length)
        # Write values in dictionary to output table
        cur = arcpy.da.InsertCursor(comp_out_tbl, all_flds)
        for row in out_rows:
            cur.insertRow(tuple(row))

    else:
        arcpy.AddMessage("All records in input table are identical to compare table")



##    print datetime.datetime.now() - start_time
            


except arcpy.ExecuteError:
    # Get the geoprocessing error messages
    msgs = arcpy.GetMessage(0)
    msgs += arcpy.GetMessages(2)

    # Write gp error messages to log
    print msgs + "\n"
    arcpy.AddError(msgs + "\n")


except:
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    # Concatenate information together concerning the error into a message string
    pymsg = tbinfo + "\n" + str(sys.exc_type)+ ": " + str(sys.exc_value)

    # Write Python error messages to log
    print pymsg + "\n"
    arcpy.AddError(pymsg + "\n")

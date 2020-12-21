import csv
import os
import sys

def get_loggify_fields_and_corresponding_data_sources(loggify_files_path):
    # reading loggify files
    files = os.listdir(loggify_files_path)
    # this dict contains fields and their corresponding data sources across all loggify filters
    loggify_fields_dict = {}
    for file in files:
        if file.endswith("loggify"):
            file1 = open(loggify_files_path + file, 'r')
            lines = file1.readlines()

            loggify_output_fields = []
            for line in lines:
                if "output" in line:
                    f = line.split("output")[1].split("from")[0]
                    field = f.strip().strip("|")
                    if field == "concatenated event_context":
                        continue
                    loggify_output_fields.append(field)
                elif "meta name" in line:
                    data_source = line.split()[-1].strip('"')
            # use set to remove duplicate elements
            loggify_output_fields = list(set(loggify_output_fields))

            # updating loggify_fields_dict
            for fds in loggify_output_fields:
                if fds not in loggify_fields_dict.keys():
                    loggify_fields_dict[fds] = []
                loggify_fields_dict[fds].append(data_source)
    return loggify_output_fields, loggify_fields_dict


def get_existing_excel_fields_and_corresponding_data_sources(csv_file):
    # this dict contains fields and their corresponding data sources across excel sheet
    excel_fields_dict = {}
    fields_in_excel = []
    with open(csv_file) as csv_f:
        csv_reader = csv.reader(csv_f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count = -1
            else:
                data_sources = row[4]
                fields_in_excel.append(row[0])
                excel_fields_dict[row[0]] = data_sources.replace(" ", "").split(",")
        print(f"Excel Fields dict: {excel_fields_dict}")
    return fields_in_excel, excel_fields_dict


def diff_between_excel_and_loggify_dicts(loggify_dict, excel_dict):
    dict_diff = {}
    for key in loggify_dict.keys():
        if key in excel_dict.keys():
            dict_diff[key] = list(set(loggify_dict[key]) - set(excel_dict[key]))
        else:
            k = key + "(new field)"
            dict_diff[k] = loggify_dict[key]
    return dict_diff


def fill_excel_sheet_with_differences(csv_file, diff_dict):
    with open('write.csv', mode='w') as updated_file:
        file_writer = csv.writer(updated_file, delimiter=',', quotechar='"')
        with open(csv_file) as csv_f:
            csv_reader = csv.reader(csv_f, delimiter=',')
            line_count = 0
            for row in csv_reader:

                if line_count == 0:
                    line_count += 1
                    file_writer.writerow([row[0], row[1], row[2], row[3], row[4], "New data sources"])
                else:
                    line_count += 1
                    new_data_sources = ','.join(diff_dict[row[0]])
                    file_writer.writerow([row[0], row[1], row[2], row[3], row[4], new_data_sources])
        for key in diff_dict.keys():
            if "(new field)" in key:
                data_sources_com_sep = ','.join(diff_dict[key])
                file_writer.writerow([key, "", "", "", "", data_sources_com_sep])


if __name__ == "__main__":
    # file is old excel_sheet_csv
    file = sys.argv[1]

    # if loggify_files_path  doesn't end with slash, add slash
    loggify_files_path = sys.argv[2]
    if not loggify_files_path.endswith("/"):
        loggify_files_path = loggify_files_path + "/"


    loggify_out_fields, loggify_dict =get_loggify_fields_and_corresponding_data_sources(loggify_files_path)

    excel_out_fields, excel_dict = get_existing_excel_fields_and_corresponding_data_sources(file)

    new_fields = list(set(loggify_out_fields) - set(excel_out_fields))


    # this include the diff in the data sources + the new fields
    different_dict = diff_between_excel_and_loggify_dicts(loggify_dict, excel_dict)
    print(f"Different Dicts: {different_dict}")

    # create new csv file
    fill_excel_sheet_with_differences(file, different_dict)
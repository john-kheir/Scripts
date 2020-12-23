import csv
import os
import sys
import uuid


def get_data_sources(csv_file):
    all_data_sources_list = []
    with open(csv_file) as csv_f:
        csv_reader = csv.reader(csv_f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count = -1
            else:
                data_sources = row[4].strip()
                # handle if there is no new data source column
                new_data_sources = row[5].strip()
                all_data_sources = data_sources + "," + new_data_sources
                all_data_sources_list.extend([ds.strip() for ds in all_data_sources.split(",") if ds])
                # remove duplicates
                all_data_sources_list = list(set(all_data_sources_list))
        return all_data_sources_list


def get_event_context_fields_per_data_source(csv_file, data_sources):
    folder_name = f"data_sources_ec_{str(uuid.uuid4())[0:8]}"
    print(f"Results folder name: {folder_name}")
    os.mkdir(folder_name)
    for data_source in data_sources:
        with open(f"{folder_name}/{data_source}.csv", mode='w') as ds_ec:
            file_writer = csv.writer(ds_ec, delimiter=',', quotechar='"')
            with open(csv_file) as csv_f:
                csv_reader = csv.reader(csv_f, delimiter=',')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        file_writer.writerow(["Count", row[0], row[3]])
                        line_count += 1
                    else:
                        if data_source in row[4] or data_source in row[5]:
                            if "event_context" in row[2]:
                                file_writer.writerow([line_count, row[0].replace("(new field)", ""), row[3]])
                                line_count += 1


if __name__ == "__main__":
    # csv file you want to work on
    file = sys.argv[1]

    data_sources = get_data_sources(file)
    get_event_context_fields_per_data_source(file, data_sources)

import csv
import json
import fastavro


def csv_to_json(csv_file, json_file):
    # reading csv file using dict-reader
    with open(csv_file, 'r') as f_csv:
        csv_dict_reader = csv.DictReader(f_csv)
        data = [row for row in csv_dict_reader]
    # writing into json file using dump method
    with open(json_file, 'w') as f_json:
        json.dump(data, f_json, indent=4)
    print("Your CSV file has been converted to JSON !!!!!")


def json_to_csv(json_file, csv_file):
    with open(json_file, 'r') as f_json:
        json_data = json.load(f_json)
    with open(csv_file, 'w', newline='') as f_csv:
        csv_dict_writer = csv.DictWriter(f_csv, fieldnames=json_data[0].keys())
        csv_dict_writer.writeheader()
        csv_dict_writer.writerows(json_data)

    print("Your JSON file has been converted to CSV !!!!!")


def csv_to_avro(csv_file, avro_file):
    # reading csv file using dict-reader
    with open(csv_file, 'r') as f_csv:
        csv_dict_reader = csv.DictReader(f_csv)
        data = [row for row in csv_dict_reader]

    fields = []
    for col in csv_dict_reader.fieldnames:
        field = {'name': col, 'type': 'string'}
        fields.append(field)

    avro_schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': fields,
    }

    with open(avro_file, 'wb') as f_avro:
        fastavro.writer(f_avro, avro_schema, data)

    print("Your CSV file has been converted to Avro !!!!!")


def avro_to_csv(avro_file, csv_file):
    with open(avro_file, 'rb') as f_avro:
        avro_reader = fastavro.reader(f_avro)
        fields = [field_dict['name'] for field_dict in avro_reader.writer_schema['fields']]
        with open(csv_file, 'w', newline='') as f_csv:
            csv_dict_writer = csv.DictWriter(f_csv, fieldnames=fields)
            csv_dict_writer.writeheader()
            for row in avro_reader:
                csv_dict_writer.writerow(row)

    print("Your Avro file has been converted to CSV !!!!!")


if __name__ == "__main__":
    csv_to_json("input/orders.csv", "output/orders.json")
    json_to_csv("input/students.json", "output/students.csv")
    csv_to_avro("input/orders.csv", "output/orders.avro")
    avro_to_csv("input/orders.avro", "output/orders.csv")

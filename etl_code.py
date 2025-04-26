import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file"

target_file = "transformed_data.csv"
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    df[['height', 'weight']] = df[['height', 'weight']].astype(float)
    return df


def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    df[['height', 'weight']] = df[['height', 'weight']].astype(float)
    return df


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame({
        'name': pd.Series(dtype='str'),
        'height': pd.Series(dtype='float'),
        'weight': pd.Series(dtype='float')})
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame([
            {
                "name": name,
                'height': height,
                'weight': weight
            }
        ])], ignore_index=True)
    return dataframe


def extract():
    extracted_dataframe = pd.DataFrame(columns=['name', 'height', 'weight']).astype({
        'name': 'object',
        'height': 'float64',
        'weight': 'float64'
    })

    for csvfile in glob.glob('*.csv'):
        if csvfile != target_file:
            extracted_dataframe = pd.concat([extracted_dataframe, extract_from_csv(csvfile)], ignore_index=True)

    for csvfile in glob.glob('*.json'):
        extracted_dataframe = pd.concat([extracted_dataframe, extract_from_json(csvfile)], ignore_index=True)

    for csvfile in glob.glob('*.xml'):
        extracted_dataframe = pd.concat([extracted_dataframe, extract_from_xml(csvfile)], ignore_index=True)

    return extracted_dataframe


def trasform(data):
    # data['height'] = data.height * 0.0254

    data['height'] = round(data.height * 0.0254, 2)

    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamps_format = "'%Y-%h-%d-%H:%M:%S'"
    now = datetime.now().strftime(timestamps_format)
    with open(log_file, 'a') as f:
        f.write(now + ',' + message + '\n')

log_progress("ETL Job Started")

extracted_data = extract()

log_progress("Extracted Phase Ended")

log_progress("Transform phase Started")

transformed_data = trasform(extracted_data)

log_progress("Transform phase Ended")

log_progress("Load phase Started")

load_data(target_file, transformed_data)

log_progress("Load phase Ended")



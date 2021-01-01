import json
import os
import sys
from cachetools import TTLCache

#######################	Operations ################################


def create_operation(client_name, key, value, **kwargs):
    ttl_value = kwargs.get('ttl', None)
    filepath = kwargs.get('filepath', ".//")
    try:
        with open(filepath+client_name+'.json', 'r+') as create_append:
            old_data = json.load(create_append)
            new_data = old_data
            creation_output = datastore_creation(
                new_data, key, value, client_name, filepath=filepath, ttl=ttl_value)
            if isinstance(creation_output, dict):
                new_data.update(creation_output)
                if 'Healthy' in validate(client_name, filepath=filepath):
                    status = dumping_util(
                        client_name, new_data, filepath=filepath)
                    if 'Healthy' in validate(client_name, filepath=filepath) and 'Dumped' in status:
                        return "Create Operation successfull-append"
                    elif 'Failed' in status:
                        return "Create Operation Failed - Data Dumping failed"
                    else:
                        with open(filepath+client_name+'.json', 'w+') as append:
                            json.dump(old_data, append)
                        return "Create Operation denied-append-(Insufficient space) | Client File execeeds 1 GB"
                else:
                    return validate(client_name, filepath=filepath)
            else:
                return creation_output

    except FileNotFoundError as e:
        return new_client_creation(client_name, key, value, filepath=filepath, ttl=ttl_value)


def client_creation(client_name, key):
    restore = client_name/key
    return restore * key


def new_client_creation(client_name, key, value, **kwargs):
    ttl_value = kwargs.get('ttl', None)
    filepath = kwargs.get('filepath', ".//")
    datastore = {}  # New client so datastore is empty
    creation_output = datastore_creation(
        datastore, key, value, client_name, filepath=filepath, ttl=ttl_value)
    if isinstance(creation_output, dict):
        status = dumping_util(client_name, creation_output, filepath=filepath)
        client_creation(0, 1)
        if 'Healthy' in validate(client_name, filepath=filepath) and 'Dumped' in status:
            return "Create Operation successfull-new"
        elif 'Failed' in status:
            return "Create Operation Failed - Data Dumping failed"
        else:
            status = reset_operation(client_name, filepath=filepath)
            client_creation(0, 1)
            if 'removed' in status:
                return "Create Operation denied-new-(Insufficient space) | Client File execeeds 1 GB"
            return validate(client_name, filepath=filepath)
    else:
        return creation_output


def datastore_creation(existing_datastore, key, value, client, **kwargs):
    ttl_value = kwargs.get('ttl', None)
    client_creation(0, 1)
    filepath = kwargs.get('filepath', ".//")
    datastore = {}
    key_existience = check_key_exist(existing_datastore, key, client)
    if 'New' in key_existience:
        status = key_value(key, value)
        if 'met' in status:
            if not isinstance(ttl_value, int):
                datastore[key] = [value, 0]
            else:
                client = ttl_create(client, key, value, ttl_value)
                datastore[key] = [value, 1]
            return datastore
        else:
            if 'Value' in status and isinstance(value, dict):
                client_creation(0, 1)
                constrain_status = "Value size limit is 16KB, But it has {} KB".format(
                    int(sys.getsizeof(value))/1000)

            elif not isinstance(value, dict):
                constrain_status = "Value's datatype should be JSON object (Dict)"

            elif 'Key' in status and isinstance(key, str):
                constrain_status = "Character limit for Key is 32, But it has {}".format(
                    len(str(key)))

            else:
                constrain_status = "Key's datatype should be String"
            return constrain_status
    else:
        constrain_status = "Key | {} | already exist , value - {} ".format(
            key, existing_datastore[key][0])
        return constrain_status

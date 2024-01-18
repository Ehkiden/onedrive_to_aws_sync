import json
import requests
import os
import boto3

'''
Purpose: 
    Goal is to sync a designated OneDrive folder with the specified S3 bucket. 
    This is done by using the Microsoft Graph delta API to query the changes since last time the function was called. (Replicates the entire OneDrive folder if no delta file can be found)
    Once the change log has been filtered, iterate the change logs and performs the following for each file:
        Download the files from the OneDrive folder to local tmp folder along with a json file containing the metadata
        Files are then uploaded to the S3 bucket and deleted from the tmp folder.
'''

# uploads or downloads specified file to/from the designated S3 bucket
def s3_ops(filename_local, filename_s3, action):
    # establish session
    session = boto3.Session(
        aws_access_key_id= os.environ['aws_bucket_key_id'],
        aws_secret_access_key= os.environ['aws_bucket_secret_key']
    )
    s3 = session.resource('s3')
    # determine and attempt action 
    try:
        if action == "upload":
            s3.meta.client.upload_file(Filename=filename_local, Bucket='rub-backoffice-invoice-prod', Key=filename_s3)
        elif action == "download":
            s3.meta.client.download_file(Filename=filename_local, Bucket='rub-backoffice-invoice-prod', Key=filename_s3)
    except Exception as e:
        # log exception
        print(f"Error occured when attempting to {action} the file {filename_local} to or from the S3 bucket. \nException details:")
        print(e)
        return False
    # log file upload
    print(f"The attempt to {action} the file {filename_local} to or from the S3 bucket was successful.")
    return True


# downloads the contents to a tmp file
def onedrive_download(headers, file_metadata):
    driveId = file_metadata['parentReference']['driveId']
    itemId = file_metadata['id']
    onedrive_url = f'https://graph.microsoft.com/v1.0/drives/{driveId}/items/{itemId}/content'
    try:
        onedrive_resp = requests.get(onedrive_url, headers=headers)
    except Exception as e:
        # log expection
        print(f"Error occured when attempting to download {file_metadata['name']} from OneDrive. \nException details:")
        print(e)
        print("Acquiring new token then try to download contents again.")
        # acquire new token then try to download contents again
        aadToken = token_gains()
        headers = { 
            'Content-Type' : 'application/json',
            'Accept' : 'application/json',
            'Authorization' : "Bearer " + aadToken
        }
        onedrive_resp = requests.get(onedrive_url, headers=headers)

    # configure vars for local folder struct
    local_folder = '/tmp/'
    local_filename = file_metadata['name']
    local_path = local_folder+local_filename

    try:
        # save both to /tmp/ folder
        with open(local_path, 'wb') as file:
            file.write(onedrive_resp.content)

        with open(local_path+'.json', 'w') as f:
            json.dump(file_metadata, f)
    except Exception as e:
        # log exception
        print(f"Error occured when attempting to save files locally. \nException details:")
        print(e)

    # use file name in metadata to get the correct files
    filename_s3 = file_metadata['parentReference']['path_relative']+'/'+local_filename

    # upload the file data then the metadata
    s3_ops(local_path, filename_s3, "upload")
    s3_ops(local_path+'.json', filename_s3+'.json', "upload")

    # file cleanup
    try:
        os.remove(local_path)
        os.remove(local_path+'.json')
    except Exception as e:
        print("Error when trying to remove files \nException details:")
        print(e)

# recursive function which gathers the delta logs by using the nextLink as the url until a deltaLink is returned
def delta_gather(url, headers, change_array):
    try:
        delta_resp = json.loads(requests.get(url, headers=headers).content)
    except requests.exceptions.HTTPError as e:
        # log expection and try to query again
        print(f"Error occured when attempting to query deltaLink. \nException details:")
        print(e)
        delta_resp = json.loads(requests.get(url, headers=headers).content)

    # prod
    target_folder = os.environ['target_folder_prod']
    target_driveID = os.environ['target_driveID_prod']
        
    if len(delta_resp['value'])>0:
        for i in delta_resp['value']:
            # filter only relevant data
            if 'path' in i['parentReference']:
                if (i['name']==target_folder) or (target_driveID in i['parentReference']['path']):
                    # use the item ID to overwrite duplicate entries as we assume the latest entry is the most current version
                    path_relative = i['parentReference']['path'][80:]
                    i['parentReference'].update({'path_relative': path_relative})
                    change_array.update({i['id']: i})

        # recursively call the function again but use the nextLink as the url
        if '@odata.nextLink' in delta_resp:
            delta_gather(delta_resp['@odata.nextLink'], headers, change_array)
        # call the function again and use the nextLink as the url
        if '@odata.deltaLink' in delta_resp:
            # save deltaLink locally for use next time 
            deltaLink_json = {
                'deltaLink': delta_resp['@odata.deltaLink']
            }
            with open('/tmp/deltaLink.json', 'w') as f:
                json.dump(deltaLink_json, f)
            # upload to s3 bucket
            if s3_ops('/tmp/deltaLink.json', 'deltaLink.json', 'upload'):
                    # if successful then attempt deleting the local file
                    try:
                        os.remove("/tmp/"+'deltaLink.json')
                    except:
                        print("Error when trying to remove files")
    return change_array

def token_gains():
    appId = os.environ['appId']
    appSecret = os.environ['appSecret']
    tenantId = os.environ['tenantId']

    # Azure Active Directory token endpoint.
    url = "https://login.microsoftonline.com/%s/oauth2/v2.0/token" % (tenantId)
    body = {
        'client_id' : appId,
        'client_secret' : appSecret,
        'grant_type' : 'client_credentials',
        'scope': 'https://graph.microsoft.com/.default'
    }

    # authenticate and obtain AAD Token for future calls
    resp = json.loads(requests.post(url, data=body).content)

    # Grab the token from the response then store it in the headers dict.
    return resp["access_token"]


def main():
    print("Script has started.")
    try:
        aadToken = token_gains()
    except Exception as e:
        # log exception
        print(f"Error occured when attempting to obtain MS token. \nException details:")
        print(e)
    
    headers = { 
        'Content-Type' : 'application/json',
        'Accept' : 'application/json',
        'Authorization' : "Bearer " + aadToken
    }

    # prod
    driveID = os.environ['driveId_prod']

    '''
    All data returned from initial query and subsequent nextLink data will contain all changes
    NOTE: while the same item can appear multiple times in the delta feed, always use the last occurance seen

    The initial delta query (with no params) returns pages of results that represent all the files in the drive
        ref: https://learn.microsoft.com/en-us/onedrive/developer/rest-api/concepts/scan-guidance?view=odsp-graph-online#crawl-and-process-by-using-delta-query        
    '''

    delta_url = f"https://graph.microsoft.com/v1.0/drives/{driveID}/root/delta"

    # check if a deltaLink value is present in S3 so it can be used for future calls 
    # attempts to download the deltaLink file in the S3
    try:
        if s3_ops('/tmp/deltaLink.json', 'deltaLink.json', 'download'):
            if os.path.exists('/tmp/deltaLink.json'):
                with open("/tmp/deltaLink.json") as f:
                    deltaLink_file = json.load(f)
                delta_url = deltaLink_file['deltaLink']
                # remove the file after getting the delta Link
                os.remove("/tmp/deltaLink.json")
    except Exception as e:
        # log exception
        print(f"Error occured when attempting to download/remove the deltaLink file. \nException details:")
        print(e)
    

    change_array = {}
    change_array = delta_gather(delta_url, headers, change_array)

    # iterate through the array
    if change_array:
        for key, value in change_array.items():
            if 'file' in value:
                onedrive_download(headers, value)

    print('Script exited successfully.')
main()
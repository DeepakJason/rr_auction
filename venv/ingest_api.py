from sre_constants import SUCCESS
import requests
import json
import datetime
from datetime import date
import time
from calendar import timegm




def start_crawl():
    crawler_id = "6374f41aa8fb230bf40eed25"

    url = "http://ec2-18-222-53-205.us-east-2.compute.amazonaws.com/api/crawlmgr/init/"

    payload={}
    headers = {
    'key': 'ed519c7c-c766-402c-a834-53e3f276b77f',
    'secret': 'b&inPb5dMpHQQQsA%Qqk',
    'crawlid': crawler_id
    }

    response = requests.request("POST", url+crawler_id, headers=headers, data=payload)

    print(response.text)
    return json.loads(response.content)


def ingest_api(asset):

    ingest_api = "https://provider-dev.api.pricingculture.com/asset/collectible"
    ingest_header = {
        "api-key": "ebbcde63-d32b-4cb5-889e-67c5a3333936",
        "secret": "Z&gA%48#H6B7pAfoYp7*",
        'content-Type': 'application/json'
    }

    api_resp = requests.post(ingest_api, headers=ingest_header, data=json.dumps(asset))
    print(api_resp)

    resp_data = json.loads(api_resp.content)
    return resp_data["asset"]


def asset_price_method(asset_id , current_bid , bid_date):

    bid_date = bid_date
    asset_price_url = "https://provider-dev.api.pricingculture.com/asset/bid/" + asset_id

    # print(asset_price_url)

    ingest_header = {
        "api-key": "ebbcde63-d32b-4cb5-889e-67c5a3333936",
        "secret": "Z&gA%48#H6B7pAfoYp7*",
        'content-Type': 'application/json'
    }
    if bid_date == None:
        # print("if bid date")
        est_time = str(datetime.datetime.utcnow() - datetime.timedelta(hours=4))
    else:
        print("else bid date")
        est_time = str(bid_date)

    asset_price_data = {
        "price": current_bid,
        "event_time": est_time + " EST",
        "custom_data": {
            "data": None
        }
    }

    asset_price_api = requests.post(asset_price_url, headers=ingest_header, data=json.dumps(asset_price_data))
    print("assetPrice", asset_price_api)
    print(est_time + "EST")


def date_source_method(asset_id , response_data):
    data_source_url = "https://provider-dev.api.pricingculture.com/data_source"
    data_source_headers = {
        "api-key": "ebbcde63-d32b-4cb5-889e-67c5a3333936",
        "secret": "Z&gA%48#H6B7pAfoYp7*",
        'content-Type': 'application/json'
    }
    data_source_obj = {
        "platform_asset_id": asset_id,
        "source_name": "Goldin",
        "json_data": None,
        "meta_data": response_data
    }

    asset_price_api = requests.post(data_source_url, headers=data_source_headers,
                                    data=json.dumps(data_source_obj))


def end_crawl(asset_counter, crawl_started , status , result_list , error_msg , error_count):
    try:
        crawler_id = "6374f41aa8fb230bf40eed25"
        result_id = crawl_started['id']
        file_location = "/home/ec2-user/script"
        status = status
        error = error_msg
        errorcnt = str(error_count)
        num_of_items = asset_counter
        print("crawler_id" , crawler_id)
        if len(result_list) >0:
            print("result_list" )
        else:
            print("result empty")
        print("file_location" , file_location)
        print("status", status)
        print("error" , error)
        print("errorcnt" , errorcnt)
        print("num_of_items"  ,num_of_items)

        if error != None:
            # url = "http://ec2-18-222-53-205.us-east-2.compute.amazonaws.com/api/crawlmgr/end/6329dab7ee4a9065d4f4084c/632ae07dee4a9065d4f4084e?status=success&numitemsfound=154&filelocation=D:/pythonProject/PycharmProjects/goldin/venv"
            url = "http://ec2-18-222-53-205.us-east-2.compute.amazonaws.com/api/crawlmgr/end/" +crawler_id + "/"+ result_id + "?numitemsfound=" + str(num_of_items) + "&status=" + status + "&filelocation=" + file_location + "&errorcnt=" + errorcnt + "&error=" + error
        else:
            url = "http://ec2-18-222-53-205.us-east-2.compute.amazonaws.com/api/crawlmgr/end/" + crawler_id + "/" + result_id + "?numitemsfound=" + str(
                num_of_items) + "&status=" + status + "&filelocation=" + file_location + "&errorcnt=" + errorcnt

        print(url)
        payload = ""
        headers = {
            'key': 'ed519c7c-c766-402c-a834-53e3f276b77f',
            'secret': 'b&inPb5dMpHQQQsA%Qqk',
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url , headers=headers, data=json.dumps(result_list))

        # print(response.text)
    except Exception as e:
        print("error in end crawler ", e)
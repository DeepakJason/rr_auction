from bs4 import BeautifulSoup
import requests
import lxml
import datetime
import json
import time
from calendar import timegm

import rr_auction as rr_parser
import ingest_api as all_ingest_api

def rr_auctions_closed_asset_parser():
    crawl_started = all_ingest_api.start_crawl()
    error_count = 0
    asset_counter = 0
    crawl_status = None
    result_list = []

    page = True
    page_no = 0
    api_sleeper_counter = 0

    try:
        while (page):
            while_loop_checker = True

            print("page number :", page_no)

            url = "https://consumer-dev.pricingculture.com/api/assets/quick_search?platform_id=in:22&columns=id,asset_url&page_num=" + str(
                page_no)

            new_session = requests.session()

            headers = {
                'api-key': '4b8e8a9c-665c-4bd6-ab4e-a0207d2142b4',
                'secret': 'KrN$TttZdl2acg$80rm*'
            }

            rr_response = new_session.get(url, headers=headers)

            rr_asset_data = json.loads(rr_response.content)

            for each_asset in rr_asset_data["objects"]:
                print(each_asset["asset_url"])
                asset_link = each_asset["asset_url"]
                asset_id = each_asset["asset_url"].split("lot-detail/")[1].split("-")[0]

                print(asset_id)


                api_sleeper_counter = api_sleeper_counter + 1

                resp_details, status_msg = rr_parser.asset_link_parser(asset_link , asset_id)

                if api_sleeper_counter == 1000:
                    time.sleep(60)
                    api_sleeper_counter = 0

                if status_msg == "success":
                    print("if success")
                    asset_counter = asset_counter + 1
                    result = {
                        "itemid": resp_details["asset_id"],
                        "itemdetailsendpoint": resp_details["asset_link"],
                        "itemcrawlstatus": resp_details["status"],
                        "error": resp_details["error"],
                        "meta": {
                            "platformassetid": resp_details["asset_id"],
                            "pcid": resp_details["pc_id"],
                            "itemname": resp_details["name"],
                            "offerst": resp_details["offering_start"],
                            "offerend": resp_details["offering_end"]
                        }
                    }
                    result_list.append(result)

                elif status_msg == "error":
                    print("else error")
                    error_count = error_count + 1
                    result = {
                        "itemid": resp_details["asset_id"],
                        "itemdetailsendpoint": resp_details["asset_link"],
                        "itemcrawlstatus": resp_details["status"],
                        "error": resp_details["error"],
                        "meta": {
                            "platformassetid": resp_details["asset_id"],
                            "pcid": resp_details["pc_id"],
                            "itemname": resp_details["name"],
                            "offerst": resp_details["offering_start"],
                            "offerend": resp_details["offering_end"]
                        }
                    }
                    result_list.append(result)

            page_no = page_no + 1

            if rr_asset_data["pagination"]["next"] == None:
                page = False
                print("break")

        if error_count > 0 and asset_counter > 0:
            crawl_status = "partial"
        elif error_count == 0 and asset_counter > 0:
            crawl_status = "success"
        elif error_count == 0 and asset_counter == 0:
            crawl_status = 'error'
        elif error_count > 0 and asset_counter == 0:
            crawl_status = 'error'

        end_crawler_resp = all_ingest_api.end_crawl(asset_counter, crawl_started, crawl_status, result_list, None,
                                                   error_count)
        print("end_crawler_resp", end_crawler_resp)
        print("asset count =" + str(asset_counter) + "and error count = " + str(error_count))


    except Exception as e:

        print("base error", e)

        all_ingest_api.end_crawl(asset_counter, crawl_started, "error", result_list, str(e), error_count)
        # print(json.dumps(result_list))

if __name__ == '__main__':

    rr_auctions_closed_asset_parser()
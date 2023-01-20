from bs4 import BeautifulSoup
import requests
import lxml
import datetime
import json
import time
from calendar import timegm

import ingest_api as all_ingest_api



def rr_auctions_parser():

    # crawl_started = all_ingest_api.start_crawl()
    # print("crawl_started", crawl_started['id'])

    error_count = 0
    asset_counter = 0
    crawl_status = None
    error_msg = None
    asset_id = None
    asset = None
    response_data = None
    result_list = []
    status_msg = None

    page = True
    page_no = 0
    api_sleeper_counter = 0
    close_asset_status_checker = False
    open_asset_status_checker = False
    try:
        while(page):

            while_loop_checker = True
            page_no = page_no + 1

            print(page_no)

            url = "https://www.rrauction.com/auctions/categories/?cat=0&sort=time&auctions=0&view=gallery&page="+ str(page_no) +"&itemQty=24&catalogLotNumber=&cp=current"

            payload = {}
            headers = {
            }
            new_session = requests.session()

            response = new_session.get(url, headers=headers, data=payload)

            print(response)

            auction_details = BeautifulSoup(response.text, 'lxml')
            auction_data = auction_details.find_all(class_='col-12 col-sm-6 col-md-3 mb-30')
            # print(auction_data)
            all_asset_links = []

            for each_auction_data in auction_data:
                # print("inside for")
                asset_link_data = each_auction_data.find(class_='auction-item__image text-center photo').find_all("a")
                # print(asset_link_data)
                for ind , each_asset_link in enumerate(asset_link_data):
                    # print(ind , each_asset_link)

                    if ind == 0:
                        asset_link = "https://www.rrauction.com/" + each_asset_link["href"]
                        # print(each_asset_link["href"])
                        asset_id = each_asset_link["href"].split("/")[3].split("-")[0]
                        print(asset_id)
                        if "auctions" in  asset_id :
                            print("asset open")
                            pass
                        else:
                            print("asset close")
                            all_asset_links.append(asset_link)

                            resp_details, status_msg =  asset_link_parser(asset_link , asset_id)

                            api_sleeper_counter = api_sleeper_counter + 1

                            if api_sleeper_counter == 100:
                                time.sleep(300)
                                api_sleeper_counter = 0

                            if status_msg == "success":
                                print("if success")
                                asset_counter = asset_counter + 1
                                clase_result = {
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
                                result_list.append(clase_result)
                                close_asset_status_checker = True
                            elif status_msg == "error":
                                print("else error")
                                error_count = error_count + 1
                                clase_result = {
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
                                result_list.append(clase_result)
                                close_asset_status_checker = False

                            while_loop_checker = False

                    elif ind == 1:
                        asset_link = "https://www.rrauction.com/" + each_asset_link["href"]
                        # print(each_asset_link["href"])
                        asset_id = each_asset_link["href"].split("/")[3].split("-")[0]
                        # print(asset_id)
                        all_asset_links.append(asset_link)

                        resp_details, status_msg =  asset_link_parser(asset_link , asset_id)

                        api_sleeper_counter = api_sleeper_counter + 1

                        if api_sleeper_counter == 100:
                            time.sleep(300)
                            api_sleeper_counter = 0

                        if status_msg == "success":
                            print("if success")
                            asset_counter = asset_counter + 1
                            open_result = {
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
                            result_list.append(open_result)
                            open_asset_status_checker = True
                        elif status_msg == "error":
                            print("else error")
                            error_count = error_count + 1
                            open_result = {
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
                            result_list.append(open_result)
                            open_asset_status_checker = False

                        while_loop_checker = False

                    # if open_asset_status_checker == True and close_asset_status_checker == False:
                    #     result_list.append(open_result)
                    #     pass
                    # if open_asset_status_checker == False and close_asset_status_checker == True:
                    #     result_list.append(clase_result)
                    #     pass
                    # if open_asset_status_checker == False and close_asset_status_checker == False:
                    #     result_list.append(clase_result)


            if while_loop_checker:
                page = False


        if error_count > 0 and asset_counter > 0:
            crawl_status = "partial"
        elif error_count == 0 and asset_counter > 0:
            crawl_status = "success"
        elif error_count == 0 and asset_counter == 0:
            crawl_status = 'error'
        elif error_count > 0 and asset_counter == 0:
            crawl_status = 'error'

        # end_crawler_resp = all_ingest_api.end_crawl(asset_counter, crawl_started, crawl_status, result_list, None,
        #                                             error_count)
        print("end_crawler_resp", end_crawler_resp)
        print("asset count =" + str(asset_counter) + "and error count = " + str(error_count))

    except Exception as e:
        print("error in main" , e)


        # all_ingest_api.end_crawl(asset_counter, crawl_started, "error", result_list, str(e), error_count)


def asset_link_parser(asset_link , asset_id):
    try:
        pc_id = None
        title = None
        current_bid = 0
        status = None
        winning_bid = None
        offering_end = None
        

        payload = {}
        headers = {}

        open_api_url = "https://www.rrauction.com/auctions/proxyauctionbiddetails?lotIndex="+ asset_id +"&bidderNumber=0"

        new_session = requests.session()

        crawler_response = new_session.get(asset_link, headers=headers, data=payload)
        print("crawler_response" , crawler_response)

        #################################################################

        open_api_response = new_session.get(open_api_url)
        # print("open_api_response" , open_api_response)

        open_api_data = json.loads(open_api_response.content)

        asset_status = open_api_data["LOTBIDDINGSTATUS"]
        print("asset_status" , asset_status)
        if asset_status == "open":
            status = "OFFERING_OPEN"
            current_bid = open_api_data["NOWAT"]
        elif asset_status == "closed":
            status = "OFFERING_CLOSED"
            current_bid = open_api_data["NOWAT"]
            winning_bid = current_bid


        end_date = open_api_data["BIDDINGEND"]

        number_of_bids = open_api_data["NUMBEROFBIDS"]
        ################################################################

        auction_details = BeautifulSoup(crawler_response.text, 'lxml')
        asset_details = auction_details.find(class_='col-12 text-left')

        asset_title_details = asset_details.h1.text.split("\n")

        lot_number = asset_title_details[1].split("#")[1]
        title = asset_title_details[2]

        # print(lot_number , title)

        image_detail = auction_details.find(class_='col-md-12 col-lg-8').a["href"]
        # print(image_detail)

        imageUrl = {
            "media_type": "image",
            "media_src": image_detail,
            "thumbnail": None,
            "caption": None,
            "is_active": True
        }

        description_details = auction_details.find(class_="bg-secondary p-4 mb-3 mb-md-0").text
        description = description_details.replace("Description","").strip()
        # print(description)

        if status == "OFFERING_OPEN":
            offering_result = "UNSOLD"
        elif status == "OFFERING_CLOSED":
            offering_result = "SOLD"


        asset = {
            "platform_asset_id": asset_id,
            "asset_type": "COLLECTIBLE",
            "pricing_type": "auction",
            "name": title,
            "description": description,
            "url": asset_link,
            "tags": None,
            "symbol": None,
            "current_bid": int(current_bid),
            "attributes": None,
            "currency_code": "USD",
            "status": status,
            "auction_start": None,
            "auction_end": end_date + " EST",
            "base_price": None,
            "bid_increment": None,
            "lot_id": lot_number,
            "lot_name": None,
            "auction_id": None,
            "auction_name": None,
            "auction_type": None,
            "media": [imageUrl],
            "custom_data": None,
            "reserve_price": None,
            "winning_bid": winning_bid,
            "final_price": None,
            "offering_result": offering_result
        }
        # print(json.dumps(asset))


        pc_id = all_ingest_api.ingest_api(asset)
        print(pc_id)

        # print("current_bid", current_bid)
        all_ingest_api.asset_price_method(asset_id, current_bid, None)


        end_utc_time = time.strptime(str(end_date), "%Y-%m-%d %H:%M:%S")
        offering_end = timegm(end_utc_time)

        resp = {
            "asset_link": asset_link,
            "asset_id": asset_id,
            "status": "success",
            "error": None,
            "pc_id": pc_id,
            "name": title,
            "offering_start": None,
            "offering_end": offering_end
        }
        # print(resp)

        return resp, "success"

    except Exception as e:
        print("error in the parser", e)
        resp = {
            "asset_link": asset_link,
            "asset_id": asset_id,
            "status": "error",
            "error": str(e),
            "pc_id": pc_id,
            "name": title,
            "offering_start": None,
            "offering_end": None
        }
        # print(resp)
        return resp, "error"

if __name__ == '__main__':

    rr_auctions_parser()



    resp , error_msg = asset_link_parser("https://www.rrauction.com/auctions/lot-detail/346116206488001-bob-dylan-signed-photograph/?cat=0" , "346116206488001")
    print(resp)
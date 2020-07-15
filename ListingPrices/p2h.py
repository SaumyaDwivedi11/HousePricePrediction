# -*- coding: utf-8 -*-
#scrapy spider for crawling the web and scraping data
import scrapy
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from scrapy.linkextractors import LinkExtractor


class P2hSpider(scrapy.Spider):
    name = 'p2h'
    allowed_domains = ['point2homes.com']
    start_urls = [ 'https://www.point2homes.com/CA/Real-Estate-Listings/BC.html']
                  
    #download_delay = 2.0
    '''
    ['https://www.point2homes.com/CA/Real-Estate-Listings/BC.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/BC.html?page=3',
    'https://www.point2homes.com/CA/Real-Estate-Listings/AB.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/AB.html?page=3',
    'https://www.point2homes.com/CA/Real-Estate-Listings/MB.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/MB.html?page=3',
                    
    'https://www.point2homes.com/CA/Real-Estate-Listings/PE.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/PE.html?page=3',
                    
    'https://www.point2homes.com/CA/Real-Estate-Listings/NB.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/NB.html?page=3',
                    
    'https://www.point2homes.com/CA/Real-Estate-Listings/ON.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/ON.html?page=3',
    'https://www.point2homes.com/CA/Real-Estate-Listings/QC.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/QC.html?page=3',
                    
    'https://www.point2homes.com/CA/Real-Estate-Listings/SK.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/SK.html?page=3',
                    
    'https://www.point2homes.com/CA/Real-Estate-Listings/NL.html',
    'https://www.point2homes.com/CA/Real-Estate-Listings/NL.html?page=3',
    'https://www.point2homes.com/CA/Real-Estate-Listings/YT.html'
                    ]
                    '''
    
    def parse(self, response):
        
        urls = response.css('div.inner-right > a::attr(href)').extract()
        for url in urls:
        	url = response.urljoin(url)
        	yield scrapy.Request(url=url, callback=self.parse_dtls)
        
        next_page= response.css('#bottom-list-pager > div > div > ul > li.next > a::attr(href)').extract_first()
        if next_page:
        	next_page=response.urljoin(next_page)
        	yield scrapy.Request(url=next_page, callback=self.parse)

        

    def parse_dtls(self, response):
        prov= response.css('#content > div > div.two-column-right > div.page-tools.details-page-tools > div.item_address > h1 > div > span:nth-child(3)::text').extract_first()
        lst_price=response.css('#details_info > div.top-info > div.price > span > span:nth-child(1)::text').extract_first()
        locality = response.css('#content > div > div.two-column-right > div.page-tools.details-page-tools > div.item_address > h1 > div > span:nth-child(2)::text').extract_first()
        postalcode = response.css('#content > div > div.two-column-right > div.page-tools.details-page-tools > div.item_address > h1 > div > span:nth-child(4)::text').extract_first()
        val5 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(5) > dt::text').extract_first()
        data5 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(5) > dd::text').extract_first()
        val4 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(4) > dt::text').extract_first()
        data4 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(4) > dd::text').extract_first()

        val3 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(3) > dt::text').extract_first()
        data3 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(3) > dd::text').extract_first()
        val2 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(2) > dt::text').extract_first()
        data2 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(2) > dd::text').extract_first()
        val1 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(1) > dt::text').extract_first()
        data1 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(1) > dd::text').extract_first()
        val6 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(6) > dt::text').extract_first()
        data6 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(6) > dd::text').extract_first()
        val7 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(7) > dt::text').extract_first()
        data7 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(7) > dd::text').extract_first()
        val8 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(8) > dt::text').extract_first()
        data8 = response.css('#details_info > div:nth-child(5) > div > dl:nth-child(8) > dd::text').extract_first()
        

        dict_val = {val1:data1, val2: data2, val3:data3, val4:data4, val5:data5, val6:data6, val7:data7, val8:data8}
        dt_add = None
        yr_built= None
        tax = None
        lot_size= None
        basement = None
        for key, data in dict_val.items():
            if key == 'Date Added':
                dt_add = data
            elif key == 'Year Built':
                yr_built = data
            elif key == 'Taxes':
                tax = data
            elif key == 'Basement':
                basement = data 
            elif key == 'Lot Size':
                lot_size = data 

        
        feat1=response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(1) > span::text').extract_first()
        fdata1 = response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(1) > strong::text').extract_first()
        feat2=response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(2) > span::text').extract_first()
        fdata2 = response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(2) > strong::text').extract_first()
        feat3=response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(3) > span::text').extract_first()
        fdata3 = response.css('#details_info > div.characteristics-cnt > ul > li:nth-child(3) > strong::text').extract_first()

        dict_bba = {feat1:fdata1, feat2:fdata2, feat3:fdata3}
        #print(dict_bba)
        bed= None
        bath= None
        area= None
        for key,data in dict_bba.items():
            if key == 'Beds':
                bed= data
            elif key == 'Baths':
                bath = data
            elif key == 'Sqft':
                area = data


        yield{
        'province':prov, 'listprice':lst_price, 'date added':dt_add, 'locality': locality, 'postal code': postalcode,
         'year built':yr_built, 'taxes':tax, 'Basement': basement, 'Lot Size': lot_size, 'Bed': bed, 'Baths': bath, 'Area': area}


        
        

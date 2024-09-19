import scrapy
from crawlsteam.items import CrawlsteamItem  # Đảm bảo import đúng tên file items

class MyscraperSpider(scrapy.Spider):
    name = "myscraper"
    allowed_domains = ["store.steampowered.com"]
    start_urls = [f"https://store.steampowered.com/search/?filter=topsellers&page={i}" for i in range(1, 50)]

    def parse(self, response):
        # Duyệt qua từng kết quả tìm kiếm
        games = response.css('a.search_result_row')

        for game in games:
            # Trích xuất thông tin cần thiết
            name = game.css('div.search_name span.title::text').get()
            release_date = game.css('div.search_released::text').get(default="").strip()
            original_price = game.css('div.discount_original_price::text').get(default=None)
            discount_price = game.css('div.discount_final_price::text').get(default=None)
            link = game.css('a::attr(href)').get()
            full_link = response.urljoin(link)
            
            # Gọi hàm parse_game để tiếp tục xử lý
            yield scrapy.Request(
                url=full_link, 
                callback=self.parse_game, 
                meta={
                    'name': name, 
                    'release_date': release_date, 
                    'original_price': original_price, 
                    'discount_price': discount_price, 
                    'link': full_link
                }
            )

    def parse_game(self, response):
        item = CrawlsteamItem()
        item['name'] = response.meta['name']
        item['release_date'] = response.meta['release_date']
        item['original_price'] = response.meta['original_price']
        item['discount_price'] = response.meta['discount_price']
        item['link'] = response.meta['link']
        
        # Lấy mô tả game
        description = response.css('div#game_area_description::text').getall()
        description_clean = ' '.join([text.strip() for text in description if text.strip()])
        item['description'] = description_clean

        # Lấy thông tin đánh giá
        review_summary = response.css('div.summary.column span.game_review_summary::text').get()
        item['review_summary'] = review_summary

        # Lấy thông tin nhà phát triển
        developer = response.css('div#developers_list a::text').get(default=None)
        item['developer'] = developer

        # Lấy thông tin nhà phát hành
        publisher = response.css('div.summary.column a::text').get(default=None)
        item['publisher'] = publisher

        # Lấy các tag của game
        game_tag = response.css('div.glance_tags_ctn.popular_tags_ctn a.app_tag::text').getall()
        game_tag = [tag.strip() for tag in game_tag]
        item['game_tag'] = game_tag

        # Lấy thêm thông tin về developer và link developer (tùy chọn)
        dev_names = response.css('div.grid_label:contains("Developer") + div.grid_content a::text').getall()
        item['dev_names'] = dev_names

        # Lấy yêu cầu hệ thống tối thiểu (Minimum Requirements)
        minimum_requirements = response.css('div.game_area_sys_req_leftCol ul.bb_ul li::text').getall()
        minimum_requirements_clean = ' '.join([req.strip() for req in minimum_requirements if req.strip()])
        item['minimum_requirements'] = minimum_requirements_clean

        # Lấy yêu cầu hệ thống đề nghị (Recommended Requirements)
        recommended_requirements = response.css('div.game_area_sys_req_rightCol ul.bb_ul li::text').getall()
        recommended_requirements_clean = ' '.join([req.strip() for req in recommended_requirements if req.strip()])
        item['recommended_requirements'] = recommended_requirements_clean
        
        # Trả về item
        yield item

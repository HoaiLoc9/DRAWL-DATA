import scrapy
import re
from crawlsteam.items import CrawlsteamItem

class MyscraperSpider(scrapy.Spider):
    name = "myscraper"
    allowed_domains = ["store.steampowered.com"]
    start_urls = [f"https://store.steampowered.com/search/?filter=topsellers&page={i}" for i in range(1, 180)]

    def parse(self, response):
        # Duyệt qua từng kết quả tìm kiếm
        games = response.css('a.search_result_row')

        for game in games:
            # Trích xuất tên game
            name = game.css('div.search_name span.title::text').get()

            # Trích xuất ngày phát hành
            release_date = game.css('div.search_released::text').get(default="").strip()

            # Trích xuất giá gốc
            original_price = game.css('div.discount_original_price::text').get(default=None)
            if original_price:
    # Sử dụng regex để loại bỏ tất cả các ký tự không phải là số, dấu phẩy hoặc dấu chấm
                cleaned_price_original = re.sub(r'[^\d.,]', '', original_price)
    
    # Loại bỏ dấu chấm ngăn cách phần nghìn
                cleaned_price_original = cleaned_price_original.replace('.', '')
    
    # Thay dấu phẩy thành dấu chấm để có thể chuyển đổi thành số thực
                cleaned_price_original = cleaned_price_original.replace(',', '.')
    
    # Chuyển giá về kiểu số thực để giữ cả phần thập phân (nếu có)
                if cleaned_price_original:
                    final_price_number_original = float(cleaned_price_original)
                else:
                    final_price_number_original = None  # hoặc giá trị mặc định khác
            else:
                final_price_number_original = None  # hoặc giá trị mặc định khác
            
            # Trích xuất giá giảm
            discount_price = game.css('div.discount_final_price::text').get(default=None)
            # Loại bỏ tất cả các ký tự không phải là số
            if discount_price:
    # Sử dụng regex để loại bỏ tất cả các ký tự không phải là số, dấu phẩy hoặc dấu chấm
                cleaned_price_discount = re.sub(r'[^\d.,]', '', discount_price)
    
    # Loại bỏ dấu chấm ngăn cách phần nghìn
                cleaned_price_discount = cleaned_price_discount.replace('.', '')
    
    # Thay dấu phẩy thành dấu chấm để có thể chuyển đổi thành số thực
                cleaned_price_discount = cleaned_price_discount.replace(',', '.')
    
    # Chuyển giá về kiểu số thực để giữ cả phần thập phân (nếu có)
                if cleaned_price_discount:
                    final_price_number_discount = float(cleaned_price_discount)
                else:
                    final_price_number_discount = None  # hoặc giá trị mặc định khác
            else:
                final_price_number_discount = None  # hoặc giá trị mặc định khác
            
            # Trích xuất liên kết
            link = game.css('a::attr(href)').get()
            full_link = response.urljoin(link)
            
            # Gọi hàm parse_game để tiếp tục xử lý
            yield scrapy.Request(
                url=full_link, 
                callback=self.parse_game, 
                meta={
                    'name': name, 
                    'release_date': release_date, 
                    'final_price_number_original': final_price_number_original, 
                    'final_price_number_discount': final_price_number_discount, 
                    'link': full_link  # Sử dụng full_link thay vì link thô
                }
            )

    def parse_game(self, response):
        item = CrawlsteamItem()
        item['name'] = response.meta['name']
        item['release_date'] = response.meta['release_date']
        item['final_price_number_original'] = response.meta['final_price_number_original']
        item['final_price_number_discount'] = response.meta['final_price_number_discount']
        # item['link'] = response.meta['link']
        
        # Lấy mô tả game
        description = response.css('div#game_area_description *::text').getall()
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
        
        # Lấy 1 tag của game
        game_tag = response.css('div.glance_tags_ctn.popular_tags_ctn a.app_tag::text').get()
        if game_tag:
            game_tag = game_tag.strip()
        item['game_tag'] = game_tag
        
        # Lấy yêu cầu hệ thống tối thiểu (Minimum Requirements)
        minimum_requirements = response.css('div.game_area_sys_req_leftCol ul.bb_ul li::text').getall()
        minimum_requirements_clean = ' '.join([req.strip() for req in minimum_requirements if req.strip()])
        # item['minimum_requirements'] = minimum_requirements_clean
        
        # Lọc RAM, ROM và CPU từ yêu cầu tối thiểu
        minimum_ram = next((req for req in minimum_requirements if 'RAM' in req), None)
        minimum_rom = next((req for req in minimum_requirements if 'storage' in req.lower() or 'available space' in req.lower()), None)
        minimum_cpu = next((req for req in minimum_requirements if 'Core i' in req), None)
        item['minimum_ram']=minimum_ram
        item['minimum_rom']=minimum_rom
        item['minimum_cpu']=minimum_cpu

        if item['final_price_number_original'] is None:
            item['final_price_number_original']=item['final_price_number_discount']
        
        if item['final_price_number_discount']=='Free':
            item['final_price_number_discount']='0'
            
        if item['review_summary'] is None:
            item['review_summary']='Mix'
        
        if not item['review_summary'] or not item['minimum_cpu'] or not item['minimum_rom'] or not item['minimum_ram']:
            return  # Bỏ qua game nếu thiếu thông tin quan trọng
        
        if item['final_price_number_discount'] is None and item['final_price_number_original'] is None:
            return
        
        yield item
            

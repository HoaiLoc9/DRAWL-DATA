import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, WebDriverException
from time import sleep
from selenium.webdriver.common.by import By

# Đường dẫn thư mục lưu trữ file CSV
output_dir = "./data"
os.makedirs(output_dir, exist_ok=True)

# Khởi tạo ChromeDriver service và options
service = Service(ChromeDriverManager().install())

chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("disable-notifications")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Khởi tạo WebDriver với Service và Options
driver = webdriver.Chrome(service=service, options=chrome_options)

def scroll_down(x=0, time_sleep=1.5, xpath_find='//*[@id="module_recommendation"]/div/div/div/h6'):
    height = driver.execute_script("return document.body.scrollHeight")
    for i in range(x, height, 5000):
        driver.execute_script("window.scrollTo({}, {});".format(i, i + 5000))
        sleep(time_sleep)
        if xpath_find:
            try:
                if driver.find_element(By.XPATH, xpath_find).is_displayed():
                    return i
            except NoSuchElementException:
                continue
    return height

def crawl(pages=range(1, 10)):  # Crawl từ trang 1 đến trang 10
    all_data = []
    for page in pages:
        # Mở URL của từng trang
        url = f"https://www.lazada.vn/dien-thoai-di-dong/?page={page}&spm=a2o4n.home.cate_1.1.1905e182tGDwoM"
        driver.get(url)
        
        # Scroll xuống để tải hết các sản phẩm
        scroll_down(x=0, xpath_find='//*[@id="root"]/div/div[2]/div[1]/div/div[1]/div[3]')
        
        # Lấy link sản phẩm
        elems = driver.find_elements(By.CSS_SELECTOR, ".RfADt [href]")
        title = [elem.text for elem in elems]
        links = [elem.get_attribute('href') for elem in elems]

        for idx in range(len(links)):
            try:
                data = {}
                data['product_name'] = title[idx]
                data['link'] = links[idx]

                # Mở URL sản phẩm
                driver.get(data['link'])

                # ================================ GET price
                try:
                    elems_price = driver.find_element(By.XPATH, '//*[@id="module_product_price_1"]/div/div/span')
                    data['price'] = elems_price.text
                except NoSuchElementException:
                    data['price'] = "N/A"

                # ================================ GET discount
                try:
                    elems_discount = driver.find_element(By.XPATH, '//*[@id="module_product_price_1"]/div/div/div/span[2]')
                    data['discount'] = elems_discount.text
                except NoSuchElementException:
                    data['discount'] = "N/A"

                # ================================ GET colours
                colours = []
                color_elements = [ele for ele in driver.find_elements(By.XPATH, '//*[@id="module_sku-select"]/div/div/div/div/div[2]/span') if ele.get_attribute('class') != 'sku-variable-img-wrap-disabled']
                for color_element in color_elements:
                    color_element.click()
                    color = driver.find_element(By.CSS_SELECTOR, ".sku-prop-content-header .sku-name").text
                    colours.append(color)

                data['colours'] = '|'.join(colours)

                # ================================ GET max quantity
                try:
                    quantity = driver.find_element(By.XPATH, '//*[@id="module_quantity-input"]/div/div/div/div[2]/span/input').get_attribute('max')
                    data['quantity'] = quantity
                except NoSuchElementException:
                    data['quantity'] = "N/A"

                # ================================ Warranty and return
                try:
                    elems_warranty_return = driver.find_element(By.XPATH, '//*[@id="module_seller_warranty"]/div/div[2]').text
                    data['warranty_return'] = elems_warranty_return
                except NoSuchElementException:
                    data['warranty_return'] = "N/A"

                # ================================ GET description
                try:
                    driver.find_element(By.XPATH, '//*[@id="module_product_detail"]/div/div/div[2]/button').click()
                except (NoSuchElementException, ElementNotInteractableException):
                    pass

                try:
                    desc = driver.find_element(By.XPATH, '//*[@id="module_product_detail"]/div/div/div[1]').text
                    data['description'] = desc
                except NoSuchElementException:
                    data['description'] = "N/A"

                # ================================ GET rate and reviews
                try:
                    count_review = driver.find_element(By.XPATH, '//*[@id="module_product_review"]/div/div/div[1]/div[2]/div/div/div[1]/div[3]').text
                    rating = driver.find_element(By.XPATH, '//*[@id="module_product_review"]/div/div/div[1]/div[2]/div/div/div[1]/div[1]').text
                    data['count_review'] = count_review
                    data['rating'] = rating
                except NoSuchElementException:
                    data['count_review'] = ""
                    data['rating'] = ""

                try:
                    review_text = [i.text for i in driver.find_elements(By.XPATH, '//*[@id="module_product_review"]/div/div/div[3]/div/div')]
                    data['review_text'] = '|'.join(review_text)
                except NoSuchElementException:
                    data['review_text'] = ""

                # ================================ GET delivery info
                try:
                    elems_delivery = driver.find_elements(By.XPATH, '//*[@id="module_seller_delivery"]/div/div/div[3]/div/div')
                    elems_delivery = [elem.text for elem in elems_delivery]
                    data['delivery'] = '|'.join(elems_delivery)
                except NoSuchElementException:
                    data['delivery'] = "N/A"

                # Lưu dữ liệu sản phẩm vào danh sách
                all_data.append(data)

                # Kiểm tra nếu đã thu thập đủ 100 sản phẩm
                if len(all_data) >= 100:
                    break

            except WebDriverException as e:
                print(f"Lỗi xảy ra với sản phẩm tại trang {page}, index {idx}: {e}")
                continue
        
        if len(all_data) >= 100:
            break

    # Lưu dữ liệu vào CSV
    df = pd.DataFrame(all_data)
    df.to_csv(os.path.join(output_dir, 'crawl.csv'), index=False)

    driver.quit()

# Gọi hàm crawl để thu thập dữ liệu
crawl(pages=range(1, 20))
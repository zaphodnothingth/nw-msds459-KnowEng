
import scrapy


class MySpider(scrapy.Spider):                                                
    name = 'from_json_list'                                                               
    def __init__(self, config_file = None, *args, **kwargs):                    
        super(MySpider, self).__init__(*args, **kwargs)                       
        with open(config_file) as f:                                            
            self._config = json.load(f)                                         
        self._url_list = self._config['url_list']                             

    def start_requests(self):                                                   
        for url in self._url_list:                                              
            yield scrapy.Request(url = url, callback = self.parse)              

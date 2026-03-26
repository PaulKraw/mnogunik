class ClientParams:
    def __init__(self, name_csv='alx', name='svai', k_gorod='k_gorod.csv', num_ads=1000, orig_t='text.txt', file_price='file_price.csv', date_f="2023-10-30", end_date=2, num_days=14, periods=None, shuffle_list=True, imgparam=None, address_to_append="https://paulkraw.ru/", CompanyName=None, ContactPhone=None, ManagerName=None, cat_wp=None, info_dict=None):
        self.name_csv = name_csv
        self.name = name
        self.cat_wp = cat_wp
        self.CompanyName = CompanyName
        self.ManagerName = ManagerName
        self.ContactPhone = ContactPhone
        self.k_gorod = k_gorod
        self.num_ads = num_ads #количесвто строк в файле
        self.orig_t = orig_t
        self.date_f = date_f
        self.end_date = end_date
        self.num_days = num_days
        self.periods = periods
        # self.shuffle_list = shuffle_list
        # self.url_imgparam = url_imgparam
        self.imgparam = imgparam
        self.file_price = file_price
        self.address_to_append = address_to_append
        
        self.info_dict = info_dict

        
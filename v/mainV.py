from scraper import download_xml_files
from xbrl_parser import main_parser

limit = 10
company_names,file_names,urls = download_xml_files("C:/Users/24adi/OneDrive/Desktop/work_stuff/Esgai/b/dataset_handler/docs/All_xml_links.xlsx", limit)

print(company_names)
print(file_names)
print(urls)

parsed_data = main_parser() 
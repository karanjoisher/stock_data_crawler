from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import TimeoutException
import os
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from urllib.request import urlretrieve
import threading
import math
from datetime import date
import traceback
import requests
from bs4 import BeautifulSoup
import json

PATH = "chromedriver.exe"
OUT = "output\\"
AD_BLOCK = None
STATE = "do_not_delete\\"

def get_chrome_driver():
	op = webdriver.ChromeOptions()
	caps = DesiredCapabilities().CHROME
	
	op.add_argument("--window-size=1920,1080")
	op.add_argument("--start-maximized")
	op.add_argument("--headless")
	if((AD_BLOCK is not None) and (len(AD_BLOCK) != 0)): op.add_argument('load-extension=' + AD_BLOCK)
	prefs = {"profile.managed_default_content_settings.images": 2}
	op.add_experimental_option("prefs", prefs)
	op.add_experimental_option("excludeSwitches", ["enable-logging"])

	ser = Service(PATH)
	caps["pageLoadStrategy"] = "eager"
	return webdriver.Chrome(desired_capabilities=caps, service=ser, options=op)

def get_row_index_for(table_rows, row_label):
	index = -1
	i = 0
	while i < len(table_rows):
		cell_values = table_rows[i].select('td')
		if len(cell_values) > 0 and row_label in cell_values[0].text:
			index = i
			break
		i = i + 1
	return index

def set_stock_detail_keys_from_row(table_name, table_rows, row_index, stock_details, stock_detail_keys):
	if (row_index < 0) or (row_index >= len(table_rows)):
		is_leverageRatioTable_and_sector_is_bank = (table_name == 'Leverage Ratio') and ('Sector' in stock_details) and (stock_details['Sector'] == 'Banks')
		is_NpaTable_and_sector_is_nonbank = (table_name == 'NPA') and ('Sector' in stock_details) and (stock_details['Sector'] != 'Banks') 
		ignore_missing_row = is_leverageRatioTable_and_sector_is_bank or is_NpaTable_and_sector_is_nonbank
		print_statement_prefix = threading.current_thread().name + " | " + stock_details['Stock Symbol'] + " : "
		if(not ignore_missing_row): print(print_statement_prefix + 'Error: Row index: ' + str(row_index) + ' not found in table ' + table_name + ' table to get following fields: ' + ",".join(stock_detail_keys) + '. Num rows in this table: ' + str(len(table_rows)))
	else:
		stock_detail_keys.insert(0, 'stub')
		cell_values = table_rows[row_index].select('td')
		i = 1
		while i < min(len(stock_detail_keys), len(cell_values)):
			stock_details[stock_detail_keys[i]] = cell_values[i].text
			i = i + 1

def share_price_on_date(stock_symbol, dte, retry=False):
	#{"s":"no_data","nextTime":1660262400}
	#{"s":"ok","t":[1597363200],"o":[1065.9],"h":[1065.9],"l":[1027.3],"c":[1034.45],"v":[10462350]} 
	#{"s":"error","errmsg":"Invalid request.Invalid Resolution."}
	print_statement_prefix = threading.current_thread().name + " | " + stock_symbol + " : "
	dte = dte.replace(hour=5, minute=30, second=00)
	timestmp = int(dte.timestamp())
	querystring = {"symbol": stock_symbol ,"resolution":"1D","from": timestmp,"to":timestmp}
	price = requests.request("GET", "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history", params=querystring)
	price = price.json()

	if price.get('s', '') == "ok":
		return price.get('c', ['NOT_FOUND'])[0]
	elif (not retry) and price.get('s', '') == "no_data":
		if 'nextTime' in price:
			timestmp = price['nextTime']
			return share_price_on_date(stock_symbol, datetime.fromtimestamp(timestmp), True)
		else:
			print(print_statement_prefix + 'Error: No share price data found for date ' + str(dte) + ' and no nextTime received in response. Response: ' + str(price))
	else:
		print(print_statement_prefix + "Error: Failed to fetch historical share prices. Response received: " + str(price))

	return 'NOT_FOUND'

def get_nifty500():
	urlretrieve('https://www1.nseindia.com/content/indices/ind_nifty500list.csv', STATE + "nifty500.csv")
	result = []
	f = open(STATE + 'nifty500.csv', "r")
	first_line = True
	for entry in f:
		if not first_line:
			csv = entry.split(',')
			result.append(csv[2].replace('\n', ''))
		first_line = False
	f.close()
	return result

def scrape(d, stock_symbol, scid_hashmap, url_hashmap):

	print_statement_prefix = threading.current_thread().name + " | " + stock_symbol + " : "

	result = {}
	result['Stock Symbol'] = stock_symbol

	#Check if scid is cached, if so, no need to use selenium
	if stock_symbol not in scid_hashmap or (len(scid_hashmap[stock_symbol]) == 0) :
		# Go to stock page
		if stock_symbol in url_hashmap and (len(url_hashmap[stock_symbol]) != 0):
			d.get(url_hashmap[stock_symbol])
		else:
			d.get('https://www.moneycontrol.com/stocks/cptmarket/compsearchnew.php?search_data=&cid=&mbsearch_str=&topsearch_type=1&search_str=' + stock_symbol)
		
		w = WebDriverWait(d, 60)
		if 'compsearchnew.php' in d.current_url:
			suggestions = w.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, '.srch_tbl tbody tr')))
			suggestion_index = -1
			for i in range(0, len(suggestions) if suggestions is not None else 0):
				suggestion_identifiers = suggestions[i].find_elements(By.CSS_SELECTOR, 'td:nth-child(2) span')
				for j in range(0, len(suggestion_identifiers)):
					suggestion_identifiers_components = suggestion_identifiers[j].text.split(':')
					if(len(suggestion_identifiers_components) != 0 and stock_symbol == suggestion_identifiers_components[-1]):
						suggestion_index = i
						break
				if suggestion_index != -1:
					break

			if suggestion_index == -1:
				result['Status'] = 'NOT_FOUND'
				print(print_statement_prefix + 'Could not find stock page for ' + stock_symbol + '. Skipping.')
				return result
			else:
				try:
					popup_cancel = WebDriverWait(d, 30).until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#wzrk-cancel')))
					popup_cancel.click()
				except Exception:
					pass
				suggestion_link = w.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#mc_mainWrapper .srch_tbl tr:nth-child(' + str(suggestion_index + 1) + ') a')))			
				suggestion_link.click()

		# Stock page loaded, grab scid
		scid = str(d.execute_script('return scid'))
		scid_hashmap[stock_symbol] = scid
		print(d.current_url)
		url_hashmap[stock_symbol] = d.current_url.replace('\n','')

	scid = scid_hashmap[stock_symbol]	

	result['Money Control ScId'] = scid
	
	# Get Overview: Market Cap, Sector
	overview = requests.request("GET", "https://priceapi.moneycontrol.com/pricefeed/nse/equitycash/" + scid)
	overview = overview.json()
	if overview.get('code', '') != '200':
		print(print_statement_prefix + 'Error in fetching overview details. Code: ' + overview.get('code', '') + ' | Message: ' + overview.get('message', ''))
	else:
		overview = overview.get('data', {})
		result['Stock Name'] = overview.get('SC_FULLNM', 'NOT_FOUND')
		result['Market Cap'] = overview.get('MKTCAP', 'NOT_FOUND')
		result['Sector'] = overview.get('main_sector', 'NOT_FOUND')

	# Get Ratios:
	querystring = {"classic":"true","referenceId":"ratios","requestType":"S","scId": scid,"frequency":""}
	ratios_html = requests.request("GET", 'https://www.moneycontrol.com/mc/widget/mcfinancials/getFinancialData', params=querystring)
	ratios_html = BeautifulSoup(ratios_html.text, 'html.parser')

	if (ratios_html.p is not None) and (ratios_html.p.string is not None) and (ratios_html.p.string == 'No Data Found'):
		print(print_statement_prefix + 'Error: No ratios data found')
	else:
		set_stock_detail_keys_from_row('Per Share Ratio', ratios_html.select('#S_per_share_ratios tbody tr'), 0, result, ['EPS', 'EPS (Y-1)', 'EPS (Y-2)'])
		return_ratio_rows = ratios_html.select('#S_return_ratios tbody tr')
		set_stock_detail_keys_from_row('Return Ratio', return_ratio_rows, 0, result, ['ROE'])
		set_stock_detail_keys_from_row('Return Ratio', return_ratio_rows, 1, result, ['ROCE'])
		leverage_ratio_rows = ratios_html.select('#S_leverage_ratios tbody tr')
		set_stock_detail_keys_from_row('Leverage Ratio', leverage_ratio_rows, 0, result, ['D/E'])
		set_stock_detail_keys_from_row('Leverage Ratio', leverage_ratio_rows, 1, result, ['ICR'])
		valuation_ratio_rows = ratios_html.select('#S_valuation_ratios tbody tr')
		set_stock_detail_keys_from_row('Valuation Ratio', valuation_ratio_rows, 0, result, ['P/E'])

	# Get Income statement
	querystring = {"classic":"true","referenceId":"income","requestType":"S","scId":scid,"frequency":"12"}
	income_statement_html = requests.request("GET", 'https://www.moneycontrol.com/mc/widget/mcfinancials/getFinancialData', params=querystring)
	income_statement_html = BeautifulSoup(income_statement_html.text, 'html.parser')
	if (income_statement_html.p is not None) and (income_statement_html.p.string is not None) and (income_statement_html.p.string == 'No Data Found'):
		print(print_statement_prefix + 'Error: No Income Statement data found')
	else:
		annual_income_stat_rows = income_statement_html.select('table tbody tr')
		set_stock_detail_keys_from_row('Annual Income Statement', annual_income_stat_rows, get_row_index_for(annual_income_stat_rows, 'Total Income'), result, ['Revenue', 'Revenue (Y-1)', 'Revenue (Y-2)'])
		set_stock_detail_keys_from_row('Annual Income Statement', annual_income_stat_rows, get_row_index_for(annual_income_stat_rows, 'Net Profit'), result, ['Profit', 'Profit (Y-1)', 'Profit (Y-2)'])
		set_stock_detail_keys_from_row('NPA', annual_income_stat_rows, get_row_index_for(annual_income_stat_rows, 'Net NPA'), result, ['Net NPA', 'Net NPA (Y-1)', 'Net NPA (Y-2)'])
		set_stock_detail_keys_from_row('NPA', annual_income_stat_rows, get_row_index_for(annual_income_stat_rows, 'Net NPA (%)'), result, ['Net NPA %', 'Net NPA % (Y-1)', 'Net NPA % (Y-2)'])

	# Get Balance Sheet
	querystring = {"classic":"true","referenceId":"balance-sheet","requestType":"S","scId":scid,"frequency":""}
	balance_sheet_html = requests.request("GET", 'https://www.moneycontrol.com/mc/widget/mcfinancials/getFinancialData', params=querystring)
	balance_sheet_html = BeautifulSoup(balance_sheet_html.text, 'html.parser')
	if (balance_sheet_html.p is not None) and (balance_sheet_html.p.string is not None) and (balance_sheet_html.p.string == 'No Data Found'):
		print(print_statement_prefix + 'Error: No Balance Sheet data found')
	else:
		assets_table_rows = balance_sheet_html.select('table tbody tr')
		set_stock_detail_keys_from_row('Assets', assets_table_rows, get_row_index_for(assets_table_rows, 'Total Assets'), result, ['Assets', 'Assets (Y-1)', 'Assets (Y-2)'])

	# Current and historical share prices
	result['Share Value'] = share_price_on_date(stock_symbol, datetime.today())
	result['Share Value (Y-1)'] = share_price_on_date(stock_symbol, datetime.today() - timedelta(days=365))
	result['Share Value (Y-2)'] = share_price_on_date(stock_symbol, datetime.today() - timedelta(days=(2*365)))

	# Promoters DII FII
	a = time.time()
	if (stock_symbol in url_hashmap):
		b = time.time()
		response = requests.request("GET", url_hashmap[stock_symbol])
		#print(print_statement_prefix + " | Fetching Share Holder HTML took " + str(time.time() - b))
		c = time.time()
		stock_page_html = BeautifulSoup(response.text, 'html.parser')
		scripts = stock_page_html.find_all('script')
		json_string = '{}'
		for script in scripts:
			source = script.text
			i = 0
			j = 0
			matched = False
			to_match = 'functionshowTrendGraph(trend_title){vartrend_jsn='
			while i < (len(source) if (source is not None) else 0):
				if j >= len(to_match):	
					matched = True
					break

				if not source[i].isspace():
					if to_match[j] != source[i]:
						break
					else:
						j = j + 1
				i = i + 1

			start_index = -1
			end_index = -1	
			if matched:
				while i < (len(source) if (source is not None) else 0):
					if source[i] == "'":
						if start_index == -1:
							start_index = i
						else:
							end_index = i
							break
					i = i + 1

			if end_index > start_index and start_index >= 0:
				json_string = source[(start_index+1): end_index]
				break

		share_holding_json = json.loads(json_string)
		if share_holding_json is None: share_holding_json = {}
		month_map = {'Jan':0, 'Feb':1, 'Mar':2, 'Apr':3, 'May':4, 'Jun':5, 'Jul':6, 'Aug':7, 'Sep':8, 'Oct':9, 'Nov':10, 'Dec':11}
		month_arr = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
		latest_month = -1
		latest_year = -1
		share_holding_keys = ['Promoter', 'FII', 'DII']
		for key in share_holding_keys:
			obj = share_holding_json.get(key, {})
			if type(obj) == dict:
				for month_year in obj:
					month_year = month_year.split(' ')
					is_latest = (int(month_year[1]) > latest_year) or ((int(month_year[1]) == latest_year) and (month_map.get(month_year[0], latest_month) > latest_month))
					if is_latest:
						latest_month = month_map[month_year[0]]
						latest_year = int(month_year[1])
			if latest_month != -1:
				break

		if latest_month != -1:
			month_year = month_arr[latest_month] + ' ' + str(latest_year)
			result['Promoter'] = share_holding_json.get('Promoter', {}).get(month_year, {}).get('Holding', 'NOT_FOUND')
			result['Pledged'] = share_holding_json.get('Promoter', {}).get(month_year, {}).get('Pledge', 'NOT_FOUND')
			result['FII'] = share_holding_json.get('FII', {}).get(month_year, {}).get('Holding', 'NOT_FOUND')
			result['DII'] = share_holding_json.get('DII', {}).get(month_year, {}).get('Holding', 'NOT_FOUND')
		else:
			print(print_statement_prefix + 'Error: Cannot find the latest month and year in share holding json: ' + str(share_holding_json))
	else:
		print(print_statement_prefix + 'Error: No url found in url_hashmap to fetch share holding patterns. Set "ignore_scid_hashmap" to "true" in config.txt to get share holding patterns for this stock.')
		result['Status'] = 'Error: No url found in url_hashmap to fetch share holding patterns. Set "ignore_scid_hashmap" to "true" in config.txt to get share holding patterns for this stock.'

	if('Status' not in result): result['Status'] = 'PROCESSED'
	return result

def get_csv_header():
	return ['Status', 'Stock Symbol', 'Stock Name', 'Market Cap', 'Sector', 'D/E', 'ICR', 'ROE', 'ROCE', 'P/E', 'EPS', 'EPS (Y-1)', 'EPS (Y-2)', 'Share Value', 'Share Value (Y-1)', 'Share Value (Y-2)', 'Profit', 'Profit (Y-1)', 'Profit (Y-2)', 'Revenue', 'Revenue (Y-1)', 'Revenue (Y-2)', 'Net NPA', 'Net NPA (Y-1)', 'Net NPA (Y-2)', 'Net NPA %', 'Net NPA % (Y-1)', 'Net NPA % (Y-2)', 'Assets', 'Assets (Y-1)', 'Assets (Y-2)', 'Promoter', 'Pledged', 'DII', 'FII']

def stock_details_to_csv(stock_details):
	fields = get_csv_header()
	values = []
	for field in fields:
		values.append(str(stock_details.get(field, 'NOT_SET')).replace(',', '').replace('\n',''))
	return ",".join(values)

def load_csv_hashmap(csv_filepath):
	hashmap = {}
	f = open(csv_filepath, "r")
	for entry in f:
		csv = entry.split(',')
		hashmap[csv[0]] = csv[1].replace('\n', '')
	f.close()	
	return hashmap

def save_csv_hashmap(hashmap, csv_filepath):
	f = open(csv_filepath, "w")
	for key in hashmap:
		csv = key + "," + hashmap[key].replace('\n', '')
		f.write(csv + "\n")
	f.close()

def scrape_arr(output_file, stock_symbols, scid_hashmap, url_hashmap):
	d = get_chrome_driver()
	processed = 0
	for stock_symbol in stock_symbols:
		print_statement_prefix = threading.current_thread().name + " | " + stock_symbol + " : "
		s = time.time()
		try:
			stock_details = scrape(d, stock_symbol, scid_hashmap, url_hashmap)
		except Exception as e:
			try:
				d.close()
			except Exception:
				pass

			try:
				d.quit()
			except Exception:
				pass

			stock_details = {}
			stock_details['Stock Symbol'] = stock_symbol
			stock_details['Status'] = 'ERROR: ' + str(e).replace(',', ';').replace('\n', '')
			print(print_statement_prefix + 'Unhandled exception. Exception Type: ' + str(type(e)) + ". Traceback: " + traceback.format_exc())
			d = get_chrome_driver()

		thread_safe_write_to_file(output_file, stock_details_to_csv(stock_details) + "\n")
		processed = processed + 1
		processed_percentage = "{:.2f}".format(processed * 100/len(stock_symbols))
		duration = "{:.2f}".format(time.time() - s)
		print(print_statement_prefix + "Processed in " + duration + "s. " + str(processed) + "/" + str(len(stock_symbols)) + " processed. (" + processed_percentage + "%)")	
	
	try:
		d.close()
	except Exception:
		pass

	try:
		d.quit()
	except Exception:
		pass

def scrape_multithreaded(output_file, stock_symbols, scid_hashmap, url_hashmap, num_threads):
	num_stocks = len(stock_symbols)
	stocks_per_thread = math.ceil(num_stocks/num_threads)
	threads = []
	start = 0
	while start < num_stocks:
		end = min(start + stocks_per_thread, num_stocks)
		thread =threading.Thread(target=scrape_arr, args=(output_file, stock_symbols[start:end], scid_hashmap, url_hashmap,), name=('T' + str(len(threads))))
		thread.start()
		threads.append(thread)
		start = end

	for thread in threads:
		print(threading.current_thread().name + ": " + 'Waiting for ' + thread.getName() + ' to finish...')
		thread.join()
		print(threading.current_thread().name + ": " + 'Thread ' + thread.getName() + ' completed')	

def thread_safe_write_to_file(output_file, string):
	lock = output_file['lock']
	file = output_file['file']
	lock.acquire()
	file.write(string)
	file.flush()
	lock.release()

def unprocessed_stock_symbols_from_csv_and_save_processed_in_output_file(csv_filepath, output_file):
	stock_symbols = []
	f = open(csv_filepath, "r")
	header_line = True
	keys = []
	stock_symbol_index = -1
	stock_status_index = -1
	for entry in f:
		entry = entry.replace('\n', '')
		csv = entry.split(',')
		if header_line:			
			keys = csv
			stock_status_index = csv.index('Status')
			stock_symbol_index = csv.index('Stock Symbol')
			header_line = False
		else:
			stock_status = csv[stock_status_index]
			if stock_status != 'PROCESSED':
				stock_symbols.append(csv[stock_symbol_index])
			else:
				stock_details = {}
				i = 0
				for key in keys:
					stock_details[key] = csv[i]
					i = i + 1
				output_file['file'].write(stock_details_to_csv(stock_details) + "\n")
		output_file['file'].flush()
	f.close()	
	return stock_symbols

def stock_symbols_from_txt(input_filepath):
	stock_symbols = []
	f = open(input_filepath, "r")
	for entry in f:
		stock_symbols.append(entry.replace('\n', ''))
	f.close()	
	return stock_symbols

if __name__ == '__main__':
	config = load_csv_hashmap('config.txt')
	AD_BLOCK = config.get('ad_block', None)

	output_filename = input('Enter output filename: ')
	output_filepath = OUT + output_filename + ".csv"

	try:
		if os.path.samefile(config['input'], output_filepath):
			print('Error: Input and output filepaths cannot be the same.')
			exit()
	except Exception:
		pass

	print('Output will be stored at ' + output_filepath)

	ignore_url_hashmap =  ('ignore_url_hashmap' in config) and (config['ignore_url_hashmap'] == 'true')
	ignore_scid_hashmap = ('ignore_scid_hashmap' in config) and (config['ignore_scid_hashmap'] == 'true') 
	url_hashmap = (load_csv_hashmap(STATE + "url_hashmap.txt") if (not ignore_url_hashmap) else {}) 
	scid_hashmap = (load_csv_hashmap(STATE + "scid_hashmap.txt") if (not ignore_scid_hashmap) else {})
	
	f = open(output_filepath, "w")
	f.write(",".join(get_csv_header()) + "\n")
	output_file = {}
	output_file['lock'] = threading.Lock()
	output_file['file'] = f

	if config['input'][-3:] == 'csv':
		stock_symbols = unprocessed_stock_symbols_from_csv_and_save_processed_in_output_file(config['input'], output_file)
	elif config['input'][-3:] == 'txt':
		stock_symbols = stock_symbols_from_txt(config['input'])
	elif config['input'] == 'nifty500':
		stock_symbols = get_nifty500()

	start = time.time()
	scrape_multithreaded(output_file, stock_symbols, scid_hashmap, url_hashmap, 4)

	if(not ignore_url_hashmap): save_csv_hashmap(url_hashmap, STATE + "url_hashmap.txt")
	if(not ignore_scid_hashmap): save_csv_hashmap(scid_hashmap, STATE + "scid_hashmap.txt")

	print("Completed in " + str(time.time() - start) + "s")		
	f.close()
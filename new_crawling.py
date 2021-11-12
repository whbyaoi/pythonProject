"""  新的爬虫文件 """
import json
import math
import time

from blockchain import blockexplorer


def transfer_blockchain_data_to_need(blockchain_data: dict):
    """ 转换从网站 blockchain 爬取的json格式数据到所需的格式"""
    page_total = math.ceil(blockchain_data['tx'].__len__() / 50)
    pages_list = []
    for page in range(page_total):
        page_dict = {'data':
                         {'list': [],
                          'page': page,
                          'page_total': page_total,
                          'page_size': 50,
                          'total_count': blockchain_data['tx'].__len__()},
                     'err_code': 200,
                     'err_no': 200,
                     'message': 'success',
                     'status': 'success'}
        tx_list = page_dict['data']['list']
        for tx_index in range(page * 50, page * 50 + 50):
            tx = blockchain_data['tx'][tx_index]
            tx_dict = {'block_hash': blockchain_data['hash'],
                       'block_height': blockchain_data['height'],
                       'block_time': blockchain_data['time'],
                       'confirmations': 0,
                       'created_at': 0,
                       'fee': tx['fee'],
                       'hash': tx['hash'],
                       'inputs': []
                       }
            for __input in tx['inputs']:
                tx_dict['inputs'].append({'addresses': [__input['prev_out']['addr']] if __input['prev_out'] else [''],
                                          'spent_by_tx': []})


def get_json_content(url):
    import time
    import requests
    import json
    import traceback
    try:
        time.sleep(10)  # 由于网站有流量限制，每次发送get请求前睡眠10秒
        r = requests.get(url)
        json_content = json.loads(str(r.content, encoding="utf-8"))
        return json_content
    except:
        print("爬取json内容时出错，url：{}".format(url))
        print('报错：{}'.format(traceback.format_exc()))
        return get_json_content(url)


# block = blockexplorer.get_block('000000000000000016f9a2c3e0f4c1245ff24856a79c34806969f5084f410680')
#
# for tx in block.transactions[1:2]:
#     for _input in tx.inputs:
#         print(_input.__dict__)

# # # 先用只读模式获取json内容
# jfp = open(r'F:\BitcoinData\640001\1_15.json', 'r')
# json_content = json.load(jfp)
# print(type(json_content))
# jfp.close()
# #
# # # 格式化写入
# fp = open(r'F:\BitcoinData\640001\1_15.json', 'w+')
# fp.write(json.dumps(json_content, sort_keys=True, indent=4))
# fp.close()

from blockcypher import get_block_overview, get_transaction_details

while 1:
    time.sleep(0.5)
    block = get_transaction_details('f854aebae95150b379cc1187d848d58225f3c4157fe992bcd166f58bd5063449')
    if block:
        print('pass')
    else:
        print('error')
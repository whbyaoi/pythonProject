import json
import time
import os
import traceback
from pathlib import Path
import requests

from map.map import *


def transfer_timestamp_to_local_time(timestamp: int) -> str:
    """
    转换时间戳成可读的时间
    :param timestamp: 时间戳
    :return: string类型的时间
    """
    time_array = time.localtime(timestamp)
    tx_formal_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return tx_formal_time


def get_json_content(url):
    try:
        time.sleep(5)  # 由于网站有流量限制，每次发送get请求前睡眠10秒
        r = requests.get(url)
        json_content = json.loads(str(r.content, encoding="utf-8"))
        return json_content
    except:
        print("爬取json内容时出错，url：{}".format(url))
        print('报错：{}'.format(traceback.format_exc()))
        return get_json_content(url)


def append_transaction_in_block(root_dir):
    """
    转换etl的导出数据格式为单独区块的数据
    :param root_dir: output文件夹
    :return:
    """
    for date_time in os.listdir(root_dir + '/blocks/'):
        block_root = root_dir + '/blocks/{}/'.format(date_time)
        transaction_root = root_dir + '/transactions/{}/'.format(date_time)
        block_file = os.listdir(block_root)[0]
        transaction_file = os.listdir(transaction_root)[0]
        target_root = root_dir + '/target/'

        t0 = time.time()

        # <editor-fold desc="main">
        block_fp = open(block_root + block_file, 'r', encoding='utf-8')
        transaction_fp = open(transaction_root + transaction_file, 'r', encoding='utf-8')
        blocks = [json.loads(i) for i in block_fp.readlines()]
        transactions = [json.loads(i) for i in transaction_fp.readlines()]

        for block in blocks:
            start = block['number'] // 2000 * 2000
            if not Path(target_root + 'BTC{}_{}'.format(start, start + 2000)).is_dir():
                os.mkdir(target_root + 'BTC{}_{}'.format(start, start + 2000))
            if not Path(target_root + 'BTC{}_{}/{}'.format(start, start + 2000, block['number'])).is_dir():
                os.mkdir(target_root + 'BTC{}_{}/{}'.format(start, start + 2000, block['number']))
            with open(target_root + 'BTC{}_{}/{}/1_1.json'.format(start, start + 2000, block['number']), 'w+',
                      encoding='utf-8') as fp:
                block['transactions'] = [t for t in transactions if t['block_hash'] == block['hash']]
                # sorted(block['transactions'], key=lambda x: x['index'])
                fp.write(json.dumps(block, indent=4))
        print('processed: {}, '.format(date_time))
        # </editor-fold>

        print(time.time() - t0)


def transfer_data_format(root_dir: str):
    """
    转换json格式二数据的为json格式一，json格式详见format文件
    :param root_dir: json格式二的数据的根目录, 注意结尾不要带 / 或者 \
    """
    for f1 in os.listdir(root_dir):
        t0 = time.time()
        for f2 in os.listdir(root_dir + '/' + f1):
            for f3 in os.listdir(root_dir + '/' + f1 + '/' + f2):
                try:
                    # 初始化新格式
                    jf = json.load(open(root_dir + '/' + f1 + '/' + f2 + '/' + f3, 'r'))
                    new_format = {
                        "data": {"list": [], "page": 1, "page_total": 1, "total_count": jf['transaction_count']}}
                    # 添加全部交易
                    for i in range(jf['transaction_count']):
                        # coinbase交易在格式二中没有输入，但是在格式一中算有一个输入
                        inputs_count = 0
                        if jf['transactions'][i]['is_coinbase']:
                            inputs_count = 1
                        else:
                            inputs_count = jf['transactions'][i]['input_count']
                        # 先添加部分信息
                        new_format['data']['list'].append({
                            "block_hash": jf['hash'],
                            "block_height": jf['number'],
                            "block_time": jf['timestamp'],
                            "confirmations": None,  # 导出数据没这个值
                            "created_at": None,  # 不知道咋算，应为int值
                            "fee": None,  # 包含交易的fee之和，得等查完inputs中的地址才能算
                            "hash": jf['transactions'][i]['hash'],
                            "inputs": [],
                            "inputs_count": inputs_count,
                            "inputs_value": None,  # 包含输入的比特币之和，得等查完inputs中的地址才能算
                            "is_coinbase": jf['transactions'][i]['is_coinbase'],
                            "is_double_spend": None,  # 不知道咋算，应为bool值
                            "is_sw_tx": None,  # 不知道咋算，应为bool值
                            "lock_time": jf['transactions'][i]['lock_time'],
                            "outputs": [],
                            "outputs_count": jf['transactions'][i]['output_count'],
                            "outputs_value": jf['transactions'][i]['output_value'],
                            "sigops": None,  # 不知道咋算，应为int值
                            "size": jf['transactions'][i]['size'],
                            "version": jf['transactions'][i]['version'],
                            "vsize": jf['transactions'][i]['virtual_size'],
                            "weight": None,  # 导出数据没有这个数据，虽然查到了计算公式但是对不上，详见format文件
                            "witness_hash": None  # 导出数据没有这个数据
                        })

                        # 添加交易中的输入信息
                        # 如果是coinbase
                        if new_format['data']['list'][-1]['is_coinbase']:
                            new_format['data']['list'][-1]['inputs'].append({
                                "prev_addresses": [""],  # 反正是不是coinbase这都先是“”，若不是coinbase得查
                                "prev_position": -1,
                                "prev_tx_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                                "prev_type": "NONSTANDARD",
                                "prev_value": 0,
                                "sequence": 0
                            })
                        else:
                            for j in range(new_format['data']['list'][-1]['inputs_count']):
                                new_format['data']['list'][-1]['inputs'].append({
                                    "prev_addresses": [""],  # 反正是不是coinbase这都先是“”，若不是coinbase得查
                                    "prev_position": jf['transactions'][i]['inputs'][j]['spent_output_index'],
                                    "prev_tx_hash": jf['transactions'][i]['inputs'][j]['spent_transaction_hash'],
                                    "prev_type": None,  # prev_address使用的脚本类型，得查
                                    "prev_value": None,  # 输入值，得查
                                    "sequence": jf['transactions'][i]['inputs'][j]['sequence']
                                })

                        # 添加交易中的输出信息
                        for k in range(new_format['data']['list'][-1]['outputs_count']):
                            if jf['transactions'][i]['outputs'][k]['type'] != 'nonstandard':
                                new_format['data']['list'][-1]['outputs'].append({
                                    "addresses": jf['transactions'][i]['outputs'][k]['addresses'],
                                    "spent_by_tx": None,  # 被使用交易的hash值，得查，可能没有被使用
                                    "spent_by_tx_position": None,  # 同上，得查
                                    "type": TYPE_MAP[jf['transactions'][i]['outputs'][k]['type']] if
                                    jf['transactions'][i]['outputs'][k]['type'] in TYPE_MAP else
                                    jf['transactions'][i]['outputs'][k]['type'],
                                    "value": jf['transactions'][i]['outputs'][k]['value']
                                })
                            else:
                                new_format['data']['list'][-1]['outputs'].append({
                                    "addresses": [""],
                                    "spent_by_tx": "",
                                    "spent_by_tx_position": -1,
                                    "type": "NULL_DATA",
                                    "value": 0
                                })
                except:
                    continue
                open(root_dir + '/' + f1 + '/' + f2 + '/' + f3, 'w').write(json.dumps(new_format, indent=4))
        print('{} processed, {}'.format(f1, time.time() - t0))


def create_index_from_tx_to_block(root_dir: str):
    """
    建立区块高度到交易hash值的索引表
    :param root_dir: json格式一的数据的根目录, 注意结尾不要带 / 或者 \
    :return: dict类型的索引表:
            {
                height1:[tx_hash1, tx_hash2, ..., tx_hashn],
                height2: [...],
                ...,
                heightn: [...]
            }
    """
    print('开始建立索引表，该过程耗时可能会过长......')
    target_dir = root_dir + '/target'
    for f1 in os.listdir(target_dir):
        rs = {}
        t0 = time.time()
        for f2 in os.listdir(target_dir + '/' + f1):
            rs[f2] = []
            for f3 in os.listdir(target_dir + '/' + f1 + '/' + f2):
                for transaction in json.load(open(target_dir + '/' + f1 + '/' + f2 + '/' + f3, 'r'))['data']['list']:
                    rs[f2].append(transaction['hash'])
        rewrite_json(root_dir + '/index_table/index_table{}.json'.format(f1[3:]), rs)
        print('{} 处理完毕, 耗时 {}s'.format(f1, round(time.time() - t0, 2)))
    print('索引表建立完毕')


def load_index(index_dir):
    rs = {}
    print('开始加载索引表')
    for f in sorted(os.listdir(index_dir)):
        t0 = time.time()
        temp_index = json.load(open(index_dir + '/' + f, 'r'))
        for index, tx_list in temp_index.items():
            for tx in tx_list:
                rs[tx] = index
        print('{} 处理完毕, 耗时 {}s'.format(f, round(time.time() - t0, 2)))
    print('索引表加载完毕')
    return rs


def complete_json_1(root_dir: str):
    """
    补全json格式一数据的字段数据，具体补全字段有：
        交易的
            fee——若该交易为coinbase，则fee为该区块所有交易的fee之和，否则为inputs_value-outputs_value
            inputs_value
        input的
            prev_address——若该交易为coinbase，则为空，否则为输入地址
            prev_type
            prev_value
        output的
            spent_by_tx
            spent_by_tx_position
    :param root_dir: json格式一的数据的根目录, 注意结尾不要带 / 或者 \
    """

    def search1(d: dict, block_height, h: str):
        # 按照最近的区块排序来搜索
        while str(block_height) in d:
            if h in d[str(block_height)]:
                return str(block_height)
            block_height -= 1
        return None

    def search2(l1: list, h: str):
        for index, t in enumerate(l1):
            if t['hash'] == h:
                return index
        return None

    def test_search(d: dict, h: str):
        if h in d:
            return d[h]
        return None

    # 加载索引表
    index_table = load_index(root_dir + '/index_table')

    # 大的要来了
    print('开始补全字段，此过程耗时过长......')
    root_dir = root_dir + '/target'
    for f1 in sorted(os.listdir(root_dir), reverse=True)[:1]:
        t0 = time.time()
        for f2 in sorted(os.listdir(root_dir + '/' + f1), reverse=True)[1:]:
            for f3 in sorted(os.listdir(root_dir + '/' + f1 + '/' + f2)):
                print('开始处理区块{}内交易'.format(root_dir + '/' + f1 + '/' + f2))
                t1 = time.time()
                # 获取待做补全处理的区块数据
                json_data = json.load(open(root_dir + '/' + f1 + '/' + f2 + '/' + f3, 'r'))
                # 遍历其中的交易
                for transaction_index, transaction in enumerate(json_data['data']['list']):
                    t2 = time.time()
                    try:
                        # coinbase的交易另做处理
                        print('开始处理交易{}/{}：{}'.format(transaction_index, json_data['data']['list'].__len__(), transaction['hash']))
                        if not transaction['is_coinbase']:
                            # 开始补全交易中的每个input
                            for input_index, _input in enumerate(transaction['inputs']):
                                search_result = test_search(index_table, _input['prev_tx_hash'])
                                # 如果在本地找到了input中的前驱交易
                                if search_result:
                                    # print('在本地找到前驱交易{}'.format(_input['prev_tx_hash']))
                                    file_name = 'BTC' + str(int(search_result) // 2000 * 2000) + '_' + str(
                                        int(search_result) // 2000 * 2000 + 2000)
                                    assert file_name in os.listdir(root_dir), '未找到区块{}所在文件夹{}'.format(search_result,
                                                                                                      file_name)
                                    prev_json = json.load(
                                        open(root_dir + '/' + file_name + '/' + str(search_result) + '/1_1.json', 'r'))
                                    index = search2(prev_json['data']['list'], _input['prev_tx_hash'])
                                    assert index is not None, '未找到前驱交易{}'.format(_input['prev_tx_hash'])
                                    prev_tx = prev_json['data']['list'][index]

                                    # 找到了前驱交易，开始补全交易中input的字段
                                    _input['prev_addresses'] = prev_tx['outputs'][_input['prev_position']]['addresses']
                                    _input['prev_type'] = prev_tx['outputs'][_input['prev_position']]['type']
                                    _input['prev_value'] = prev_tx['outputs'][_input['prev_position']]['value']
                                    # 开始补全前驱交易中的output字段
                                    prev_tx['outputs'][_input['prev_position']]['spent_by_tx'] = transaction['hash']
                                    prev_tx['outputs'][_input['prev_position']]['spent_by_tx_position'] = input_index
                                    # 补全完毕，保存前驱交易的json文件
                                    rewrite_json(root_dir + '/' + file_name + '/' + str(search_result) + '/1_1.json',
                                                 prev_json)
                                # 没在本地找到前驱交易，联网查找
                                else:
                                    continue
                                    # print('未在本地找到前驱交易{}'.format(_input['prev_tx_hash']))
                                    prev_tx = get_json_content(
                                        'https://chain.api.btc.com/v3/tx/{}?verbose=2'.format(_input['prev_tx_hash']))[
                                        'data']
                                    # 联网找到了前驱交易，开始补全交易中input的字段
                                    _input['prev_addresses'] = prev_tx['outputs'][_input['prev_position']]['addresses']
                                    _input['prev_type'] = prev_tx['outputs'][_input['prev_position']]['type']
                                    _input['prev_value'] = prev_tx['outputs'][_input['prev_position']]['value']
                    except:
                        print('error')
                        print(root_dir + '/' + file_name + '/' + str(search_result) + '/1_1.json')
                        exit()
                    # 补全交易中的inputs_value和fee字段
                    transaction['inputs_value'] = sum([int(i['prev_value']) for i in transaction['inputs']])
                    transaction['fee'] = transaction['inputs_value'] - transaction['outputs_value']
                    print('区块{}的交易{}处理完毕，耗时{}s'.format(f2, transaction['hash'], round(time.time()-t2, 2)))
                rewrite_json(root_dir + '/' + f1 + '/' + f2 + '/' + f3, json_data)
                print('{} 处理完毕, 耗时 {}s'.format(root_dir + '/' + f1 + '/' + f2, time.time() - t1))
        print('{} 处理完毕, 耗时 {}s'.format(f1, time.time() - t0))
    print('字段补全完毕')



def complete_json_2(root_dir: str):
    """
    用于补全coinbase交易中的fee和inputs_value
    :param root_dir:
    :return:
    """
    for f1 in os.listdir(root_dir):
        t0 = time.time()
        for f2 in os.listdir(root_dir + '/' + f1):
            for f3 in os.listdir(root_dir + '/' + f1 + '/' + f2):
                json_data = json.load(open(root_dir + '/' + f1 + '/' + f2 + '/' + f3, 'r'))
                coinbase_transaction = json_data['data']['list'][0]
                coinbase_transaction['inputs_value'] = 0
                coinbase_transaction['fee'] = sum([i['fee'] for i in json_data['data']['list'][1:]])
                rewrite_json(root_dir + '/' + f1 + '/' + f2 + '/' + f3, json_data)
        print('{} 处理完毕, 耗时 {}s'.format(f1, time.time() - t0))


def rewrite_json(json_dir: str, json_content, indent=4):
    fp = open(json_dir, 'w+')
    fp.write(json.dumps(json_content, indent=indent))
    fp.close()


def format_json(json_file: str, indent=4):
    # 先用只读模式获取json内容
    jfp = open(json_file, 'r')
    json_content = json.load(jfp)
    jfp.close()
    # 格式化写入
    fp = open(json_file, 'w+')
    fp.write(json.dumps(json_content, sort_keys=True, indent=indent))
    fp.close()


if __name__ == '__main__':
    complete_json_1('/home/weirencs/output')

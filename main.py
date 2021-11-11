import copy
from collections import defaultdict
import graph_tool.all as gt
import numpy as np
import json
import os
import time
import traceback
from typing import DefaultDict, List, Optional

'''
务必运行添加节点属性
'''
# reverse_map = defaultdict(lambda: {})


# import pickle

# with open("revmap.pkl","wb") as f:
#     pickle.dump(reverse_map,f)



# 时间格式转换为时间戳
def time_reverse(temp_time: int):
    import time
    time_style = time.strptime(str(temp_time), '%Y-%m-%d %H:%M:%S')
    result_time = time.mktime(time_style)
    return result_time


def transfer_timestamp_to_readable_time(timestamp: int) -> str:
    """
    转换时间戳成可读的时间
    :param timestamp: 时间戳
    :return: string类型的时间
    """
    time_array = time.localtime(timestamp)
    tx_formal_time = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return tx_formal_time


def add_TxNode_property(directed_graph):
    tx_hash = directed_graph.new_vertex_property("string")
    directed_graph.vp["tx_hash"] = tx_hash
    tx_inputs_count = directed_graph.new_vertex_property("int")
    directed_graph.vp["tx_inputs_count"] = tx_inputs_count
    tx_inputs_value = directed_graph.new_vertex_property("double")
    directed_graph.vp["tx_inputs_value"] = tx_inputs_value
    tx_outputs_count = directed_graph.new_vertex_property("int")
    directed_graph.vp["tx_outputs_count"] = tx_outputs_count
    tx_outputs_value = directed_graph.new_vertex_property("double")
    directed_graph.vp["tx_outputs_value"] = tx_outputs_value
    tx_block_height = directed_graph.new_vertex_property("int")
    directed_graph.vp["tx_block_height"] = tx_block_height
    tx_block_time = directed_graph.new_vertex_property("int")
    directed_graph.vp["tx_block_time"] = tx_block_time
    tx_fee = directed_graph.new_vertex_property("double")
    directed_graph.vp["tx_fee"] = tx_fee
    tx_size = directed_graph.new_vertex_property("int")
    directed_graph.vp["tx_size"] = tx_size


def add_AdsNode_property(directed_graph):
    ads_address = directed_graph.new_vertex_property("string")
    directed_graph.vp["address"] = ads_address
    ads_prev_type = directed_graph.new_vertex_property("string")
    directed_graph.vp["prev_type"] = ads_prev_type
    ads_next_type = directed_graph.new_vertex_property("string")
    directed_graph.vp["next_type"] = ads_next_type


def add_edge_property(directed_graph):
    edge_value = directed_graph.new_edge_property("double")
    directed_graph.ep["value"] = edge_value
    edge_time = directed_graph.new_edge_property("int")
    directed_graph.ep["time"] = edge_time


# 添加区块交易节点
def add_TxNode_coinbase(directed_graph, hash: str, inputs_count: int, outputs_count: int, outputs_value: float, block_height: int,
                        block_time: int, fee: float, size: int):
    #     添加节点以及属性
    coinbase_node = directed_graph.add_vertex()
    graph.gp["transaction_dict"][hash] = directed_graph.vertex_index[coinbase_node]
    directed_graph.vp["tx_hash"][coinbase_node] = hash
    directed_graph.vp["tx_inputs_count"][coinbase_node] = inputs_count
    directed_graph.vp["tx_outputs_count"][coinbase_node] = outputs_count
    directed_graph.vp["tx_outputs_value"][coinbase_node] = outputs_value
    directed_graph.vp["tx_block_height"][coinbase_node] = block_height
    directed_graph.vp["tx_block_time"][coinbase_node] = block_time
    directed_graph.vp["tx_fee"][coinbase_node] = fee
    directed_graph.vp["tx_size"][coinbase_node] = size
    # print(directed_graph.list_properties())


def add_TxNode(directed_graph, hash, inputs_count, inputs_value, outputs_count, outputs_value, block_height, block_time, fee, size):
    #     添加交易节点以及属性
    tx_node = directed_graph.add_vertex()
    graph.gp["transaction_dict"][hash] = directed_graph.vertex_index[tx_node]
    directed_graph.vp["tx_hash"][tx_node] = hash
    directed_graph.vp["tx_inputs_count"][tx_node] = inputs_count
    directed_graph.vp["tx_inputs_value"][tx_node] = inputs_value
    directed_graph.vp["tx_outputs_count"][tx_node] = outputs_count
    directed_graph.vp["tx_outputs_value"][tx_node] = outputs_value
    directed_graph.vp["tx_block_height"][tx_node] = block_height
    directed_graph.vp["tx_block_time"][tx_node] = block_time
    directed_graph.vp["tx_fee"][tx_node] = fee
    directed_graph.vp["tx_size"][tx_node] = size
    # print(directed_graph.list_properties())


def add_inputs_AdsNode(directed_graph, prev_address, prev_type):
    # 创建输入地址节点
    # if gt.find_vertex(directed_graph, "address", prev_address)
    if prev_address not in graph.gp["account_dict"]:
        ads_inputs_node = directed_graph.add_vertex()
        graph.gp["account_dict"][prev_address] = directed_graph.vertex_index[ads_inputs_node]
        directed_graph.vp["address"][ads_inputs_node] = prev_address
        directed_graph.vp["prev_type"][ads_inputs_node] = prev_type
    else:
        ads_index = graph.gp["account_dict"][prev_address]
        #         print("add prev_type")
        if directed_graph.vp["prev_type"][ads_index] == '':
            # print(1)
            directed_graph.vp["prev_type"][ads_index] = prev_type


def add_outputs_AdsNode(directed_graph, address, next_type):
    # 创建输出地址节点
    if address not in graph.gp["account_dict"]:
        ads_outputs_node = directed_graph.add_vertex()
        graph.gp["account_dict"][address] = directed_graph.vertex_index[ads_outputs_node]
        directed_graph.vp["address"][ads_outputs_node] = address
        directed_graph.vp["next_type"][ads_outputs_node] = next_type
    else:
        ads_index = graph.gp["account_dict"][address]
        # print("add next_type")
        # print(str(directed_graph.vp["next_type"][ads_index]))
        # print(directed_graph.vp["next_type"][ads_index].__len__())
        if directed_graph.vp["next_type"][ads_index] == '':
            # print(1)
            directed_graph.vp["next_type"][ads_index] = next_type


# 添加输入地址——>交易hash的边
def add_inputs_AdsEdge(directed_graph, AdsNode, TxNode, prev_value, block_time):
    Ads_index = graph.gp["account_dict"][AdsNode]
    Tx_index = graph.gp["transaction_dict"][TxNode]
    #     print("添加输入边")
    # 添加输入地址——>交易hash的边
    new_edge = directed_graph.add_edge(directed_graph.vertex(Ads_index), directed_graph.vertex(Tx_index))
    #     print(new_edge)
    directed_graph.ep["value"][new_edge] = prev_value
    directed_graph.ep["time"][new_edge] = block_time


# 添加交易hash——>输出地址的边
def add_outputs_AdsEdge(directed_graph, TxNode, AdsNode, value, block_time):
    # add outputs_edge
    Tx_index = graph.gp["transaction_dict"][TxNode]
    Ads_index = graph.gp["account_dict"][AdsNode]
    new_edge = directed_graph.add_edge(directed_graph.vertex(Tx_index), directed_graph.vertex(Ads_index))
    directed_graph.ep["value"][new_edge] = value
    directed_graph.ep["time"][new_edge] = block_time


def process_single_folder(directed_graph, path_folder):
    # function of reading one json file
    files_json = os.listdir(path_folder)
    flag_coinbase = True  # set it before the start of a folder loop

    for file in files_json:
        json_file = open(path_folder + "/" + file, 'r')
        json_content = json_file.read()
        json_dict = json.loads(json_content)

        for index, i in enumerate(json_dict["data"]["list"]):
            try:
                # coinbase transaction
                if 'is_coinbase' in i and i['is_coinbase']:
                    '''Tx node attribute
                    '''

                    tx_hash = i["hash"]
                    tx_input_count = i["inputs_count"]
                    tx_output_count = i["outputs_count"]
                    tx_output_value = i["outputs_value"] / 100000000
                    tx_block_height = i["block_height"]
                    # tx_time = i["block_time"]
                    # time_array = time.localtime(tx_time)
                    # 不对时间戳进行处理
                    tx_formal_time = i["block_time"]
                    tx_fee = 0
                    tx_size = i["size"]  # bytes
                    #                     print("添加版本交易")
                    add_TxNode_coinbase(directed_graph, tx_hash, tx_input_count, tx_output_count, tx_output_value,
                                        tx_block_height,
                                        tx_formal_time, tx_fee, tx_size)

                    '''Output node and edge attribute
                    '''

                    for j in i["outputs"]:
                        if j["type"] != "NULL_DATA":
                            output_address = j["addresses"][0]
                            output_type = j["type"]
                            add_inputs_AdsNode(directed_graph, output_address, output_type)

                            output_edge_value = j["value"] / 100000000
                            output_edge_time = tx_formal_time
                            add_outputs_AdsEdge(directed_graph, tx_hash, output_address, output_edge_value, output_edge_time)

                        else:
                            continue
                    flag_coinbase = False

                # general transaction
                else:
                    '''Tx node attribute
                    '''

                    tx_hash = i["hash"]
                    tx_input_count = i["inputs_count"]
                    tx_input_value = i["inputs_value"] / 100000000
                    tx_output_count = i["outputs_count"]
                    if "outputs_value" in i:
                        tx_output_value = i["outputs_value"] / 100000000  # 判断outputs_value是否存在
                        tx_block_height = i["block_height"]
                        # tx_time = i["block_time"]
                        # time_array = time.localtime(tx_time)
                        tx_formal_time = i["block_time"]
                        if "fee" in i:
                            tx_fee = i["fee"] / 100000000
                            tx_size = i["size"]  # bytes
                        else:
                            tx_fee = 0
                            tx_size = i["size"]  # bytes
                        #                         print("添加交易")
                        add_TxNode(directed_graph, tx_hash, tx_input_count, tx_input_value, tx_output_count,
                                   tx_output_value,
                                   tx_block_height, tx_formal_time, tx_fee, tx_size)

                    else:
                        break
                    '''Input node and edge attribute
                    '''

                    for j in i["inputs"]:
                        input_address = j["prev_addresses"][0]
                        if "prev_type" in j:
                            input_type = j["prev_type"]
                            #                             print("添加输入点")
                            add_inputs_AdsNode(directed_graph, input_address, input_type)
                            input_edge_value = j["prev_value"] / 100000000
                            input_edge_time = tx_formal_time
                            add_inputs_AdsEdge(directed_graph, input_address, tx_hash, input_edge_value, input_edge_time)
                        else:
                            continue

                    '''Output node and edge attribute
                    '''

                    for j in i["outputs"]:
                        if "type" in j:
                            if j["type"] != "NULL_DATA":
                                output_address = j["addresses"][0]
                                output_type = j["type"]
                                add_outputs_AdsNode(directed_graph, output_address, output_type)

                                output_edge_value = j["value"] / 100000000
                                output_edge_time = tx_formal_time
                                add_outputs_AdsEdge(directed_graph, tx_hash, output_address, output_edge_value,
                                                    output_edge_time)

                            else:
                                continue
                        else:
                            continue
            except:
                print("错误文件：{}".format(path_folder + "/" + file))
                print(i["hash"])
                exstr = traceback.format_exc()
                print(exstr)
                continue
        json_file.close()


def traverse_folder(directed_graph, start_num, end_num, folder_path):
    for num in range(start_num, end_num + 1):
        real_path = folder_path + str(num)
        print(num)
        process_single_folder(directed_graph, real_path)


'''
*********第一**********
'''


# 1.1.1 某个交易的输入or输出金额大于M的交易个数
def check_transaction_amount_out(graph, amount: Optional[float] = 2):
    num_trans = 0
    num_total = 0
    for v in graph.iter_vertices():
        if graph.vp["tx_hash"][v] != '':
            num_total += 1
    vertex_index = gt.find_vertex_range(graph, graph.vp["tx_outputs_value"], (amount, 100000))
    print(num_total)
    num_trans = vertex_index.__len__()
    return num_trans, num_total


# 1.1.2 中存在输入or输出个数大于M的交易 （忽略矿池交易）
def compare_in_out_edge(graph, num: Optional[int] = 3):
    num_total = 0
    num_tran = 0
    for v in graph.iter_vertices():
        if graph.vp["tx_hash"][v] != '':
            num_total += 1
            #             for s,t in graph.iter_in_edges(v):
            #                 print(s,t)
            #             print(graph.get_in_degrees([v]))
            in_edge_num = (graph.get_in_degrees([v]))[0]
            out_edge_num = (graph.get_out_degrees([v]))[0]
            if (in_edge_num > num) or (out_edge_num > num):
                print('交易 {} 的输出或输入个数大于 {} 分别为：输入|输出 :{}|{}'.format(graph.vp["tx_hash"][v], num, in_edge_num, out_edge_num))


# 1.1.3 check ratio 存在【某个输入or某个输出/交易总金额】的比值大于P的交易
def check_transaction_ratio(graph, ratio: Optional[float] = 0.75):
    for v in graph.iter_vertices():
        if graph.vp["tx_hash"][v] == "":
            continue
        total_value = graph.vp["tx_outputs_value"][v]
        # 计算输入边
        for s, t, p in graph.iter_in_edges(v, [graph.ep["value"]]):
            #             print(graph.vp["tx_hash"][v])
            #             if p is None:
            #                 continue
            temp_ratio = p / graph.vp["tx_inputs_value"][v]
            if temp_ratio >= ratio:
                print("{}交易存在单个输入金额大于{}比例的交易,比例为{:.3f}".format(graph.vp["tx_hash"][v], ratio, temp_ratio))
                continue
        # 计算输出边
        for s, t, p in graph.iter_out_edges(v, [graph.ep["value"]]):
            #             print(graph.vp["tx_hash"][v])
            temp_ratio = p / graph.vp["tx_outputs_value"][v]
            if temp_ratio >= ratio:
                print("{}交易存在单个输出金额大于{}比例的交易,比例为{:.3f}".format(graph.vp["tx_hash"][v], ratio, temp_ratio))
                continue


# 1.1.4 总金额（即输入和）大于M的交易，以及单个输入或者输出大于N
def check_transaction_amount_total(graph, MAX_money: Optional[float] = 3.0, single_MAX: Optional[float] = 3.0):
    single_out_bigger, out_bigger = 0, 0  # 分别为交易总金额大于M的计算数据  out为发送  in接收
    single_in_bigger, in_bigger = 0, 0
    for v in graph.iter_vertices([graph.vp["tx_hash"]]):
        # print(v)
        # print(graph.vp["tx_inputs_value"][v])
        if v[1] != '':
            if graph.vp["tx_inputs_value"][v[0]] > MAX_money:
                in_bigger += 1
            if graph.vp["tx_outputs_value"][v[0]] > MAX_money:
                out_bigger += 1
            for s, t, p in graph.iter_in_edges(v[0], [graph.ep["value"]]):
                if p >= single_MAX:
                    single_in_bigger += 1
                    # print("{}交易存在单个输入金额大于{}的交易,为{}".format(graph.vp["tx_hash"][v[0]], single_MAX, p))

            # 计算输入边
            for s, t, p in graph.iter_out_edges(v[0], [graph.ep["value"]]):
                if p >= single_MAX:
                    single_out_bigger += 1
                    # print("{}交易存在单个输入金额大于{}的交易,为{}".format(graph.vp["tx_hash"][v[0]], single_MAX, p))
        else:
            continue
    print("单个输入输出大于{}的交易个数分别为：{}|{},交易总输入输出大于{}的交易个数为{}|{}".format(single_MAX, single_in_bigger, single_out_bigger
                                                                   , MAX_money, in_bigger, out_bigger))


# 1.1.5 及1.1.6手续费大于M的交易 add 检测交易的手续函数
def check_fee(graph, string):
    index = graph.gp["transaction_dict"][string]
    return graph.vp["tx_fee"][index]


def check_transaction_amount(graph, Tran_nodes: str):
    try:
        index = graph.gp["transaction_dict"]
        tran_amount = graph.vp["tx_outputs_value"][index]
        return float(tran_amount)

    except BaseException as e:
        print("函数check_transaction_amount在地址{}发生错误".format(Tran_nodes))


def compare_check_fee(graph, amount: Optional[float] = 0.001, Tran_nodes: Optional[str] = None):
    try:
        if Tran_nodes is None:
            for v in graph.iter_vertices([graph.vp["tx_outputs_value"], graph.vp["tx_fee"]]):
                if v[1] == '':
                    continue
                if v[2] > amount:
                    print("交易{}的fee大于{}，为{}".format(graph.vp["tx_hash"][v[0]], amount, v[2]))
        else:
            tx_fee = check_fee(graph, Tran_nodes)
            if tx_fee > amount:
                print("查询交易{}的fee大于{}，为{}".format(Tran_nodes, amount, tx_fee))
    except BaseException as e:
        if Tran_nodes is None:
            print("交易{}错误".format(v[1]))
        else:
            print("查询错误{}".format(Tran_nodes))


def compare_fee_ratio(graph, Tran_nodes: str, standard_ratio: Optional[float] = 0.01):
    try:
        if Tran_nodes is None:
            for v in graph.iter_vertices([graph.vp["tx_outputs_value"], graph.vp["tx_fee"]]):
                if v[1] == '':
                    continue
                if v[2] / v[1] > standard_ratio:
                    print("交易{}的fee与交易金额比例大于{}，为{:.3f}".format(graph.vp["tx_hash"][v[0]], standard_ratio, v[2] / v[1]))
        else:
            tx_fee = check_fee(graph, Tran_nodes)
            tx_amount = check_transaction_amount(graph, Tran_nodes)
            if tx_fee / tx_amount > standard_ratio:
                print("查询交易{}的fee与交易金额比例大于{}，为{:.3f}".format(Tran_nodes, standard_ratio, tx_fee / tx_amount))
    except BaseException as e:
        if Tran_nodes is None:
            print("交易{}错误".format(v[1]))
        else:
            print("查询错误{}".format(Tran_nodes))


## 1.1.7 【Max（输入or输出）-Min（输入or输出）】大于M的交易
def transaction_extreme_deviation(graph, Tran_nodes: Optional[str] = None, Standard: Optional[float] = 1):
    def check_list_difference(list_1):
        if list_1.__len__() != 0:
            return max(list_1) - min(list_1)
        else:
            return 0.0

    def compare_diff(graph, index):
        list_in = []
        list_out = []
        for list in graph.get_in_edges(index, [graph.ep["value"]]):
            list_in.append(list[2])
        diff_in = check_list_difference(list_in)
        for list in graph.get_out_edges(index, [graph.ep["value"]]):
            list_out.append(list[2])
            # print(list_in)
        diff_out = check_list_difference(list_out)
        if (diff_in > Standard) or (diff_out > Standard):
            print("地址{}的输入输出金额最大最小值差大于{}，为{}|{}".format(graph.vp["tx_hash"][index], Standard, diff_in, diff_out))

    try:
        if Tran_nodes is None:
            for v in graph.iter_vertices([graph.vp["tx_hash"]]):
                if v[1] == '':
                    continue
                compare_diff(graph, v[0])
        else:
            tx_index = graph.gp["transaction_dict"][Tran_nodes]
            compare_diff(graph, tx_index)
    except BaseException as e:
        print('函数transaction_extreme_deviation发生错误，交易节点'.format(Tran_nodes))
        print(e)
        exstr = traceback.format_exc()
        print(exstr)


# 1.1.8 交易中输入or输出的全部金额的标准差or平方差（或其他类似指标？；paper lxf）
def check_transaction_statistical_index(graph, Tran_nodes: str):
    import math

    def get_average(records):
        """
        平均值
        """
        return sum(records) / len(records)

    def get_variance(records):
        """
        方差 反映一个数据集的离散程度
        """
        average = get_average(records)
        return sum([(x - average) ** 2 for x in records]) / len(records)

    def get_standard_deviation(records):
        """
        标准差 == 均方差 反映一个数据集的离散程度
        """
        variance = get_variance(records)
        return math.sqrt(variance)

    def compute_math(list1):
        list2 = []
        list2.append(get_average(list1))
        list2.append(get_standard_deviation(list1))
        return list2

    def edge2list(edges):
        list1 = []
        if edges.__len__() == 0:
            return list1
        for edge in edges:
            list1.append(edge[2])
        return list1

    tx_index = graph.gp["transaction_dict"][Tran_nodes]
    in_edges = graph.get_in_edges(tx_index, [graph.ep["value"]])
    out_edges = graph.get_out_edges(tx_index, [graph.ep["value"]])
    list_in = compute_math(edge2list(in_edges))
    list_out = compute_math(edge2list(out_edges))
    return list_in, list_out


# 1.2.1 某个输入or某个输出的金额大于M的地址
def input_or_output_value_more_than_M(graph, M: float = 2.0, address: str = None):
    if address:
        v = graph.gp["account_dict"][address]
        for e in v.in_edges():
            if graph.ep['value'][e] > M:
                print('地址{}在交易{}中作为输出方，金额大于{}，金额为{}'.format(address, graph.vp['tx_hash'][e.source()], M, graph.ep['value'][e]))
        for e in v.out_edges():
            if graph.ep['value'][e] > M:
                print('地址{}在交易{}中作为输入方，金额大于{}，金额为{}'.format(address, graph.vp['tx_hash'][e.target()], M, graph.ep['value'][e]))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            for e in graph.vertex(v_index).in_edges():
                if graph.ep['value'][e] > M:
                    print('地址{}在交易{}中作为输出方，金额大于{}，金额为{}'.format(v_address, graph.vp['tx_hash'][e.source()], M, graph.ep['value'][e]))
            for e in graph.vertex(v_index).out_edges():
                if graph.ep['value'][e] > M:
                    print('地址{}在交易{}中作为输入方，金额大于{}，金额为{}'.format(v_address, graph.vp['tx_hash'][e.target()], M, graph.ep['value'][e]))


# 1.2.2 总输入or总输出金额大于M的地址
def input_or_output_sum_value_more_than_M(graph, M: float = 10, address: str = None):
    if address:
        v = graph.gp["account_dict"][address]
        sum_value = sum([graph.ep['value'][e] for e in v.in_edges()])
        if sum_value > M:
            print('地址{}的输入金额之和大于{}，金额为{}'.format(address, M, sum_value))
        sum_value = sum([graph.ep['value'][e] for e in v.out_edges()])
        if sum_value > M:
            print('地址{}的输出金额之和大于{}，金额为{}'.format(address, M, sum_value))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            sum_value = sum([graph.ep['value'][e] for e in graph.vertex(v_index).in_edges()])
            if sum_value > M:
                print('地址{}的输入金额之和大于{}，金额为{}'.format(v_address, M, sum_value))
            sum_value = sum([graph.ep['value'][e] for e in graph.vertex(v_index).out_edges()])
            if sum_value > M:
                print('地址{}的输出金额之和大于{}，金额为{}'.format(v_address, M, sum_value))


# 1.2.3 【总输入+总输出】金额大于M的地址
def sum_of_input_and_output_more_than_M(graph, M: float = 20.0, address: str = None):
    if address:
        v = graph.gp["account_dict"][address]
        sum_value = sum([graph.ep['value'][e] for e in v.in_edges()]) + sum([graph.ep['value'][e] for e in v.out_edges()])
        if sum_value > M:
            print('地址{}的输出金额和输入金额之和大于{}，金额为{}'.format(address, M, sum_value))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            sum_value = sum([graph.ep['value'][e] for e in graph.vertex(v_index).in_edges()]) + sum([graph.ep['value'][e] for e in graph.vertex(v_index).out_edges()])
            if sum_value > M:
                print('地址{}的输出金额和输入金额之和大于{}，金额为{}'.format(v_address, M, sum_value))


# 1.2.4 总输入/总输出的比值大于P的地址
def ratio_of_input_and_output_value_more_than_P(graph, P: float = 1.0, address: str = None):
    if address:
        v = graph.gp["account_dict"][address]
        if sum([graph.ep['value'][e] for e in v.out_edges()]) != 0:
            ratio = sum([graph.ep['value'][e] for e in v.in_edges()]) / sum([graph.ep['value'][e] for e in v.out_edges()])
            if ratio > P:
                print('地址{}的输入金额和输出金额的比值大于{}，比值为{}'.format(address, P, round(ratio, 4)))
        else:
            print('地址{}的总输出金额为0，无法计算'.format(address))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            if sum([graph.ep['value'][e] for e in graph.vertex(v_index).out_edges()]) != 0:
                ratio = sum([graph.ep['value'][e] for e in graph.vertex(v_index).in_edges()]) / sum([graph.ep['value'][e] for e in graph.vertex(v_index).out_edges()])
                if ratio > P:
                    print('地址{}的输入金额和输出金额的比值大于{}，比值为{}'.format(v_address, P, round(ratio, 4)))
            else:
                print('地址{}的出度为0，无法计算'.format(v_address))


# 2.2.3 【入度or出度/总度】的比值大于P的地址
def ratio_of_in_or_out_and_sum_degree_more_than_P(graph, P: float = 1.0, address: str = None):
    if address:
        v_index = graph.gp["account_dict"][address]
        ratio = graph.vertex(v_index).in_degree() / (graph.vertex(v_index).in_degree() + graph.vertex(v_index).out_degree())
        if ratio > P:
            print('地址{}的入度/总度大于{}，比值为{}'.format(address, P, round(ratio, 4)))
        ratio = graph.vertex(v_index).out_degree() / (graph.vertex(v_index).in_degree() + graph.vertex(v_index).out_degree())
        if ratio > P:
            print('地址{}的出度/总度大于{}，比值为{}'.format(address, P, round(ratio, 4)))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            ratio = graph.vertex(v_index).in_degree() / (graph.vertex(v_index).in_degree() + graph.vertex(v_index).out_degree())
            if ratio > P:
                print('地址{}的入度/总度大于{}，比值为{}'.format(v_address, P, round(ratio, 4)))
            ratio = graph.vertex(v_index).out_degree() / (graph.vertex(v_index).in_degree() + graph.vertex(v_index).out_degree())
            if ratio > P:
                print('地址{}的出度/总度大于{}，比值为{}'.format(v_address, P, round(ratio, 4)))


# 2.2.4 【入度/出度】的比值大于P的地址
def ratio_of_in_and_out_degree_more_than_P(graph, P: float = 1.0, address: str = None):
    if address:
        v_index = graph.gp["account_dict"][address]
        if graph.vertex(v_index).out_degree() != 0:
            ratio = graph.vertex(v_index).in_degree() / graph.vertex(v_index).out_degree()
            if ratio > P:
                print('地址{}的入度/出度大于{}，比值为{}'.format(address, P, round(ratio, 4)))
            ratio = graph.vertex(v_index).out_degree() / graph.vertex(v_index).out_degree()
            if ratio > P:
                print('地址{}的出度/出度大于{]，比值为{}'.format(address, P, round(ratio, 4)))
        else:
            print('地址{}的出度为0，无法计算'.format(address))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            if graph.vertex(v_index).out_degree() != 0:
                ratio = graph.vertex(v_index).in_degree() / graph.vertex(v_index).out_degree()
                if ratio > P:
                    print('地址{}的入度/出度大于{}，比值为{}'.format(v_address, P, round(ratio, 4)))
                ratio = graph.vertex(v_index).out_degree() / graph.vertex(v_index).in_degree() + graph.vertex(v_index).out_degree()
                if ratio > P:
                    print('地址{}的出度/出度大于{}，比值为{}'.format(v_address, P, round(ratio, 4)))
            else:
                print('地址{}的出度为0，无法计算'.format(v_address))


# 2.2.5 【|入度-出度|】大于N的地址
def differ_of_in_and_out_degree_more_than_N(graph, N: int = 1, address: str = None):
    if address:
        v_index = graph.gp["account_dict"][address]
        differ = abs(graph.vertex(v_index).in_degree() - graph.vertex(v_index).out_degree())
        if differ > N:
            print('地址{}的|入度-总度|大于{}，比值为{}'.format(address, N, differ))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            differ = abs(graph.vertex(v_index).in_degree() - graph.vertex(v_index).out_degree())
            if differ > N:
                print('地址{}的|入度-总度|大于{}，比值为{}'.format(v_address, N, differ))


def create_n_hop_set(graph, address, n_hop_times_limit):
    """
    此函数用于创建从地址address起始的n-hop图，
    :param graph: 图
    :param address: 地址
    :param n_hop_times_limit: n次数
    :return: set类型，存储了n-hop图中点集的地址或hash值
    """
    # 所求连通子图中的点集S
    n_hop_vertex_set = set()
    # 点集V‘=copy(V)
    vertex_address_copy = set(list(graph.gp["account_dict"].keys())) | set(list(graph.gp["transaction_dict"].keys()))
    # 初始化扩散集合T=[v]
    temp_n_hop_vertex_set = set(list([address]))
    n_hop_times = 0
    # 如果扩散集合T有新的点可继续扩散，并且仍有可用扩散次数
    while temp_n_hop_vertex_set.__len__() > 0 and n_hop_times < n_hop_times_limit:
        # S = S ∪ T 将扩散集合T中的点并入连通子图S
        n_hop_vertex_set.update(temp_n_hop_vertex_set)
        # V’ = V‘ - T 去除点集V’中包含扩散集合T中的点，防止回环
        vertex_address_copy.difference_update(temp_n_hop_vertex_set)
        # 浅拷贝扩散集合T得到副本T‘，为计算新的扩散集合做准备
        copy_temp_n_hop_vertex_set = copy.copy(temp_n_hop_vertex_set)
        # 清空扩散集合T
        temp_n_hop_vertex_set.clear()
        # 选取扩散集合中的一点并开始扩散操作
        for temp_address in copy_temp_n_hop_vertex_set:
            # 判断扩散集合中的点所代表的实体是地址还是交易
            if n_hop_times % 2 == 0:
                # 扩散集合中的点的实体是address，需要获取graph中的vertex实体，该实体类型是地址
                vertex_entity = graph.vertex(graph.gp["account_dict"][temp_address])
                # 对vertex实体计算新的扩散集合T并求并集，新的扩散集合中实体是交易
                temp_n_hop_vertex_set.update(set([graph.vp['tx_hash'][next_vertex] for next_vertex in vertex_entity.all_neighbors()]) - n_hop_vertex_set)
            else:
                # 扩散集合中的点的实体是address，需要获取graph中的vertex实体，该实体类型是交易
                vertex_entity = graph.vertex(graph.gp["transaction_dict"][temp_address])
                # 对vertex实体计算新的扩散集合T并求并集，新的扩散集合中实体是地址
                temp_n_hop_vertex_set.update(set([graph.vp['address'][next_vertex] for next_vertex in vertex_entity.all_neighbors()]) - n_hop_vertex_set)
        # 扩散次数+1
        n_hop_times += 1
        # print('扩散次数{}'.format(n_hop_times))
        # print('扩散集合T{}'.format(temp_n_hop_vertex_set))
        # print('连通子图S{}'.format(n_hop_vertex_set))
    return n_hop_vertex_set


# 2.2.6 相关联的交易数大于N的地址
# 交易和地址相关联的定义：
# 在无向n-hop图中，对于地址a和交易t，若存在从地址a到交易n的路径，则称a与t是相关联的
def related_txs_more_than_N(graph, address: str = None, N: int = 1, n_hop_times_limit: int = 2):
    n_hop_vertex_set = create_n_hop_set(graph, address, n_hop_times_limit)
    related_txs = [address for address in n_hop_vertex_set if address in graph.gp["transaction_dict"]]
    if related_txs.__len__() > N:
        print("以地址{}生成的{}-hop图中，与之相关联的交易数超过了{}，为{}个".format(address, n_hop_times_limit, N, related_txs.__len__()))


# 2.2.7 相关联的地址数大于N的地址
# 地址和地址相关联的定义：
# 在无向n-hop图中，对于地址a1和地址a2，若存在从地址a1到地址a2的路径，则称a1与a2是相关联的
def related_addresses_more_than_N(graph, address: str = None, N: int = 1, n_hop_times_limit: int = 2):
    n_hop_vertex_set = create_n_hop_set(graph, address, n_hop_times_limit)
    related_addresses = [address for address in n_hop_vertex_set if address in graph.gp["account_dict"]]
    if related_addresses.__len__() > N:
        print("以地址{}生成的{}-hop图中，与之相关联的地址数超过了{}，为{}个".format(address, n_hop_times_limit, N, related_addresses.__len__()))


# 3.1 生命周期【最晚活跃时间-最早活跃时间】大于N的地址
def life_circle_more_than_N_days(graph, N: int = 1, N_type: int = 1, address: str = None):
    if address:
        earliest_time = time.time()
        latest_time = 0
        for edge in graph.vertex(graph.gp["account_dict"][address]).all_edges():
            earliest_time = min(earliest_time, graph.ep['time'][edge])
            latest_time = max(latest_time, graph.ep['time'][edge])
        if N_type == 0 and (latest_time - earliest_time) / 86400 > N:
            print('地址{}的生命周期大于{}天, 为{}天'.format(address, N, round((latest_time - earliest_time) / 86400, 2)))
        elif N_type == 1 and (latest_time - earliest_time) / 3600 > N:
            print('地址{}的生命周期大于{}小时, 为{}小时'.format(address, N, round((latest_time - earliest_time) / 3600, 2)))
        elif N_type == 2 and (latest_time - earliest_time) / 60 > N:
            print('地址{}的生命周期大于{}分钟, 为{}分钟'.format(address, N, round((latest_time - earliest_time) / 60, 2)))
        else:
            print('时间类型输入错误')
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            earliest_time = time.time()
            latest_time = 0
            for edge in graph.vertex(v_index).all_edges():
                earliest_time = min(earliest_time, graph.ep['time'][edge])
                latest_time = max(latest_time, graph.ep['time'][edge])
            if N_type == 0 and (latest_time - earliest_time) / 86400 > N:
                print('地址{}的生命周期大于{}天, 为{}天'.format(v_address, N, round((latest_time - earliest_time) / 86400, 2)))
            elif N_type == 1 and (latest_time - earliest_time) / 3600 > N:
                print('地址{}的生命周期大于{}小时, 为{}小时'.format(v_address, N, round((latest_time - earliest_time) / 3600, 2)))
            elif N_type == 2 and (latest_time - earliest_time) / 60 > N:
                print('地址{}的生命周期大于{}分钟, 为{}分钟'.format(v_address, N, round((latest_time - earliest_time) / 60, 2)))
            else:
                print('时间类型输入错误')
                break


# 3.2 活跃周期大于N的地址
# 活跃的定义：若某地址a在某天参与过一次及以上次数的交易，则称地址在这天是活跃的
def activity_circle_more_than_N(graph, N: int = 1, address: str = None):
    def timestamp_to_need(timestamp):
        time_array = time.localtime(timestamp)
        return time.strftime("%Y%m%d", time_array)

    if address:
        activity_days = set()
        for edge in graph.vertex(graph.gp["account_dict"][address]).all_edges():
            day = timestamp_to_need(graph.ep['time'][edge])
            activity_days.add(day)
        if activity_days.__len__() > N:
            print('地址{}的活跃周期大于{}，为{}'.format(address, N, activity_days.__len__()))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            activity_days = set()
            for edge in graph.vertex(v_index).all_edges():
                day = timestamp_to_need(graph.ep['time'][edge])
                activity_days.add(day)
            if activity_days.__len__() > N:
                print('地址{}的活跃周期大于{}，为{}'.format(v_address, N, activity_days.__len__()))


# 3.3 【活跃周期/生命周期】的比值（大于P的地址）
def ratio_of_activity_and_life_circle_more_than_P(graph, P: float = 0.1, address: str = None):
    def timestamp_to_need(timestamp):
        time_array = time.localtime(timestamp)
        return time.strftime("%Y%m%d", time_array)

    if address:
        activity_days = set()
        earliest_time = time.time()
        latest_time = 0
        for edge in graph.vertex(graph.gp["account_dict"][address]).all_edges():
            day = timestamp_to_need(graph.ep['time'][edge])
            activity_days.add(day)
            earliest_time = min(earliest_time, graph.ep['time'][edge])
            latest_time = max(latest_time, graph.ep['time'][edge])
        activity_circle = activity_days.__len__()
        life_circle = (latest_time - earliest_time) / 86400
        ratio = activity_circle / life_circle
        if ratio > P:
            print('地址{}的活跃周期/生命周期的比值大于{}，为{}'.format(address, P, round(ratio, 2)))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            activity_days = set()
            earliest_time = time.time()
            latest_time = 0
            for edge in graph.vertex(v_index).all_edges():
                day = timestamp_to_need(graph.ep['time'][edge])
                activity_days.add(day)
                earliest_time = min(earliest_time, graph.ep['time'][edge])
                latest_time = max(latest_time, graph.ep['time'][edge])
            activity_circle = activity_days.__len__()
            life_circle = (latest_time - earliest_time) / 86400
            ratio = activity_circle / life_circle
            if ratio > P:
                print('地址{}的活跃周期/生命周期的比值大于{}，为{}'.format(v_address, P, round(ratio, 2)))


# 3.4 最长交易时间间隔/生命周期的比值大于P的地址
def ratio_of_longest_tx_interval_and_life_circle_more_than_P(graph, P: float = 0.1, address: str = None):
    if address:
        v_index = graph.gp["account_dict"][address]
        longest_tx_interval = 0
        # <editor-fold desc="求最长交易时间间隔">
        tx_dict = {}
        for tx in graph.vertex(v_index).all_neighbors():
            tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
        tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
        if tx_dict.__len__() <= 1:
            print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(address))
            return
        prev_tx = next_tx = None
        for index in range(tx_dict.__len__() - 1):
            if tx_dict[index][1] - tx_dict[index + 1][1] > longest_tx_interval:
                longest_tx_interval = tx_dict[index][1] - tx_dict[index + 1][1]
                prev_tx = tx_dict[index][0]
                next_tx = tx_dict[index + 1][0]
        # </editor-fold>

        life_circle = 0
        # <editor-fold desc="求生命周期">
        earliest_time = time.time()
        latest_time = 0
        for edge in graph.vertex(v_index).all_edges():
            earliest_time = min(earliest_time, graph.ep['time'][edge])
            latest_time = max(latest_time, graph.ep['time'][edge])
        life_circle = latest_time - earliest_time
        # </editor-fold>

        ratio = longest_tx_interval / life_circle
        if ratio > P:
            print('地址{}的最长交易时间间隔/生命周期的比值大于{}，为{}'.format(address, P, round(ratio, 2)))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            longest_tx_interval = 0
            # <editor-fold desc="求最长交易时间间隔">
            tx_dict = {}
            for tx in graph.vertex(v_index).all_neighbors():
                tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
            tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
            if tx_dict.__len__() <= 1:
                print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(v_address))
                continue
            prev_tx = next_tx = None
            for index in range(tx_dict.__len__() - 1):
                if tx_dict[index][1] - tx_dict[index + 1][1] > longest_tx_interval:
                    longest_tx_interval = tx_dict[index][1] - tx_dict[index + 1][1]
                    prev_tx = tx_dict[index][0]
                    next_tx = tx_dict[index + 1][0]
            # </editor-fold>

            life_circle = 0
            # <editor-fold desc="求生命周期">
            earliest_time = time.time()
            latest_time = 0
            for edge in graph.vertex(v_index).all_edges():
                earliest_time = min(earliest_time, graph.ep['time'][edge])
                latest_time = max(latest_time, graph.ep['time'][edge])
            life_circle = latest_time - earliest_time
            # </editor-fold>

            ratio = longest_tx_interval / life_circle
            if ratio > P:
                print('地址{}的最长交易时间间隔/生命周期的比值大于{}，为{}'.format(v_address, P, round(ratio, 2)))


# 3.5 地址交易时间间隔最大值or最小值or平均值
def minimum_or_maximum_or_average_of_tx_interval(graph, address: str = None):
    v_index = graph.gp["account_dict"][address]
    tx_interval_maximum = 0
    tx_interval_minimum = time.time()
    tx_interval_average = 0
    max_prev_tx = max_next_tx = None
    min_prev_tx = min_next_tx = None
    tx_dict = {}
    for tx in graph.vertex(v_index).all_neighbors():
        tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
    tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
    if tx_dict.__len__() <= 1:
        print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(address))
        return
    intervals = [tx_dict[index][1] - tx_dict[index + 1][1] for index in range(tx_dict.__len__() - 1)]
    tx_interval_average = round(sum(intervals) / intervals.__len__(), 2)
    for index, interval in enumerate(intervals):
        if interval > tx_interval_maximum:
            tx_interval_maximum = interval
            max_prev_tx = tx_dict[index][0]
            max_next_tx = tx_dict[index + 1][0]
        if interval < tx_interval_minimum:
            tx_interval_minimum = interval
            min_prev_tx = tx_dict[index][0]
            min_next_tx = tx_dict[index + 1][0]
    print('地址{}的交易时间间隔（单位：s）\n'
          '最大值为{}，来自于交易\n\t{}和\n\t{}\n'
          '最小值为{}，来自于交易\n\t{}和\n\t{}\n'
          '平均值为{}'.format(address, tx_interval_maximum, max_prev_tx, max_next_tx, tx_interval_minimum, min_prev_tx, min_next_tx, tx_interval_average))


# 3.6 地址交易【时间间隔最大值/时间间隔最小值】的比值大于P的地址
def ratio_of_tx_interval_maximum_and_minimum_more_than_P(graph, P: float = 10, address: str = None):
    if address:
        v_index = graph.gp["account_dict"][address]
        tx_interval_maximum = 0
        tx_interval_minimum = time.time()
        tx_dict = {}
        for tx in graph.vertex(v_index).all_neighbors():
            tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
        tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
        if tx_dict.__len__() <= 1:
            print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(address))
            return
        intervals = [tx_dict[index][1] - tx_dict[index + 1][1] for index in range(tx_dict.__len__() - 1)]
        tx_interval_maximum = max(intervals)
        tx_interval_minimum = min(intervals)
        if tx_interval_minimum == 0:
            print('地址{}的交易时间间隔最小值为0，无法计算'.format(address))
            return
        elif tx_interval_maximum / tx_interval_minimum > P:
            print('地址{}的交易时间间隔最大值/时间间隔最小值大于{}，为{}'.format(address, P, round(tx_interval_maximum / tx_interval_minimum), 2))
    else:
        for v_address, v_index in graph.gp["account_dict"].items():
            tx_interval_maximum = 0
            tx_interval_minimum = time.time()
            tx_dict = {}
            for tx in graph.vertex(v_index).all_neighbors():
                tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
            tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
            if tx_dict.__len__() <= 1:
                print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(v_address))
                continue
            intervals = [tx_dict[index][1] - tx_dict[index + 1][1] for index in range(tx_dict.__len__() - 1)]
            tx_interval_maximum = max(intervals)
            tx_interval_minimum = min(intervals)
            if tx_interval_minimum == 0:
                print('地址{}的交易时间间隔最小值为0，无法计算'.format(v_address))
                continue
            elif tx_interval_maximum / tx_interval_minimum > P:
                print('地址{}的交易时间间隔最大值/时间间隔最小值大于{}，为{}'.format(v_address, P, round(tx_interval_maximum / tx_interval_minimum), 2))


# 3.7 地址交易时间间隔的标准差or平方差
def standard_deviation_and_variance_of_tx_interval(graph, address: str = None):
    v_index = graph.gp["account_dict"][address]
    tx_interval_maximum = 0
    tx_interval_minimum = time.time()
    tx_dict = {}
    for tx in graph.vertex(v_index).all_neighbors():
        tx_dict[graph.vp['tx_hash'][tx]] = graph.vp['tx_block_time'][tx]
    tx_dict = sorted(tx_dict.items(), key=lambda kv: kv[1], reverse=True)
    if tx_dict.__len__() <= 1:
        print('地址{}的直接相关交易只有一个，无法计算交易时间间隔'.format(address))
        return
    intervals = [tx_dict[index][1] - tx_dict[index + 1][1] for index in range(tx_dict.__len__() - 1)]
    print('地址{}的交易时间间隔的\n'
          '标准差为{}\n'
          '方差为{}'.format(address, np.std(intervals), np.var(intervals)))


'''
main
'''
if __name__ == '__main__':
    graph = gt.Graph()
    graph.graph_properties["account_dict"] = graph.new_graph_property("object", {})
    graph.graph_properties["transaction_dict"] = graph.new_graph_property("object", {})

    add_TxNode_property(graph)
    add_AdsNode_property(graph)
    add_edge_property(graph)
    #     add_TxNode_property(graph)
    folder_path = os.getcwd().replace('\\', '/') + '/data/json_data/'
    start_folder = 634000
    end_folder = 634004
    traverse_folder(graph, start_folder, end_folder, folder_path)
    # print()
    # print(graph.gp["transaction_dict"])
    # print(graph.gp["account_dict"])
    graph.save("test11.xml.gz")

    # ↓测试代码
    graph.list_properties()
    # ratio_of_tx_interval_maximum_and_minimum_more_than_P(graph, 1)
    related_txs_more_than_N(graph, 'bc1qwqdg6squsna38e46795at95yu9atm8azzmyvckulcc7kytlcckxswvvzej', 10, 5)
    related_addresses_more_than_N(graph, 'bc1qwqdg6squsna38e46795at95yu9atm8azzmyvckulcc7kytlcckxswvvzej', 10, 5)
    # input_or_output_value_more_than_M(graph)
    # input_or_output_sum_value_more_than_M(graph)
    # sum_of_input_and_output_more_than_M(graph)
    # ratio_of_input_and_output_value_more_than_P(graph)
    # print(type(graph.vp["next_type"][0]))
    # print(graph.vp["next_type"][0])
    # tx_hash = graph.new_vertex_property("string")
    # for v in graph.iter_vertices():
    #     print(v)
    #     print(graph.vp["address"][v])
#     print(gt.find_vertex(graph, graph.vp["tx_hash"], 0))

from pyspark import SparkContext

from slicing.base.SparkedNode import SparkedNode
from slicing.base.slicer import union, opt_fun
from slicing.base.top_k import Topk
from slicing.sparked import sparked_utils
from slicing.sparked.sparked_union_slicer import update_nodes
from slicing.sparked.sparked_utils import update_top_k


def join_enum_fun(node_a, list_b, predictions, f_l2, debug, alpha, w, loss_type, cur_lvl, top_k):
    x_size = len(predictions)
    nodes = []
    for node_i in range(len(list_b)):
        flag = sparked_utils.slice_join_nonsense(node_i, node_a, cur_lvl)
        if not flag:
            new_node = SparkedNode(predictions, f_l2)
            parents_set = set(new_node.parents)
            parents_set.add(node_i)
            parents_set.add(node_a)
            new_node.parents = list(parents_set)
            parent1_attr = node_a.attributes
            parent2_attr = list_b[node_i].attributes
            new_node_attr = union(parent1_attr, parent2_attr)
            new_node.attributes = new_node_attr
            new_node.name = new_node.make_name()
            new_node.calc_bounds(cur_lvl, w)
            # check if concrete data should be extracted or not (only for those that have score upper
            # and if size of subset is big enough
            to_slice = new_node.check_bounds(top_k, x_size, alpha)
            if to_slice:
                new_node.process_slice(loss_type)
                new_node.score = opt_fun(new_node.loss, new_node.size, f_l2, x_size, w)
                # we decide to add node to current level nodes (in order to make new combinations
                # on the next one or not basing on its score value
                if new_node.check_constraint(top_k, x_size, alpha) and new_node.key not in top_k.keys:
                    nodes.append(new_node)
                    top_k.add_new_top_slice(new_node)
                elif new_node.check_bounds(top_k, x_size, alpha):
                    nodes.append(new_node)
            else:
                if new_node.check_bounds(top_k, x_size, alpha):
                        nodes.append(new_node)
            if debug:
                new_node.print_debug(top_k, cur_lvl)
    return nodes


def flatten(l):
    flat_list = set()
    for sublist in l:
        for item in sublist:
            flat_list.add(item)
    return flat_list


def parallel_process(all_features, predictions, loss, sc, debug, alpha, k, w, loss_type, enumerator):
    top_k = Topk(k)
    cur_lvl = 0
    levels = []
    all_features = list(all_features)
    first_tasks = sc.parallelize(all_features)
    SparkContext.broadcast(sc, top_k)
    first_level = first_tasks.mapPartitions(lambda features: sparked_utils.make_first_level(features, predictions, loss, top_k,
                                                                                 alpha, k, w, loss_type)).collect()
    update_top_k(first_level, top_k, alpha, predictions)
    SparkContext.broadcast(sc, top_k)
    SparkContext.broadcast(sc, first_level)
    levels.append(first_level)
    cur_lvl = cur_lvl + 1
    top_k.print_topk()
    SparkContext.broadcast(sc, top_k)
    # checking the first partition of level. if not empty then processing otherwise no elements were added to this level
    while len(levels[cur_lvl - 1]) > 0:
        cur_lvl_res = {}
        nodes_list = []
        partitions = sc.parallelize(levels[cur_lvl - 1])
        mapped = partitions.mapPartitions(lambda nodes: sparked_utils.nodes_enum(nodes, levels[cur_lvl - 1], predictions, loss,
                                                            top_k, alpha, k, w, loss_type, cur_lvl, debug, enumerator))\
            .reduce(lambda a, b: a + b)
        result = update_nodes(mapped, nodes_list, cur_lvl_res, w)
        cur_lvl_res = result[0]
        nodes_list = result[1]
        update_top_k(list(nodes_list), top_k, alpha, predictions)
        SparkContext.broadcast(sc, mapped)
        levels.append(mapped)
        SparkContext.broadcast(sc, top_k)
        cur_lvl = cur_lvl + 1
        top_k.print_topk()
        print("Level " + str(cur_lvl) + " had " + str(len(levels) * (len(levels) - 1)) +
              " candidates but after pruning only " + str(len(mapped)) + " go to the next level")
    print("Program stopped at level " + str(cur_lvl))
    print()
    print("Selected slices are: ")
    top_k.print_topk()

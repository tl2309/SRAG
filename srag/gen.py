# -*- encoding: utf-8 -*-
# @Time : 2024/9/13 0:49
# @Author: TLIN

# import sys
#
# sys.path.append('/home/linteng/SGraphQA/wikirag')

from wikirag.qageneration import AutoQA
from wikirag.transformation import Transfer


def main():
    path = '../benchmarks'

    auto = AutoQA(path)
    # # auto.gen_s_query()
    #
    # tran = Transfer()
    # tran.trans(path)

    auto.gen_answer()


if __name__ == '__main__':
    main()

# -*- encoding: utf-8 -*-

# import sys

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

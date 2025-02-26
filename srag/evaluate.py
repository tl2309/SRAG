# -*- encoding: utf-8 -*-
# @Time : 2024/9/13 0:33
# @Author: TLIN

from wikirag.qageneration import AutoQA
# from wikirag.transformation import Transfer
from llm.gpt import GPT
from llm.llama3 import Llama3
from wikirag.db_utils import dbutils
import pandas
import sqlite3
import pandas as pd
import json
import os

def agent(model, prompt):
    gpt = GPT()
    llama = Llama3()
    # prompt = prompt
    if model in ['GPT-4', 'GPT-3.5']:
        return gpt(prompt, model)
    else:
        return llama.run(prompt)

def check(question, groundrtrue, answer):
    prompt = '''
     The question is {}, the true answer is {},the given answer is{}.if the given answer meet the true answer,output 'True', if not meet, output'False'.just output the the 'True' or 'False', no other words.
    '''.format(question, groundrtrue, answer)

    return agent('GPT-3.5', prompt)

def gpt_eval(path, outfile='multi_entities_gpt.jsonl', qfile='multi_entities_query.jsonl', model='GPT-4'):
    idx = 0
    cout_t = 0
    fout_qa = open(os.path.join(path, outfile), 'a')
    with open(os.path.join(path, qfile), 'r') as file:
        # Read each line in the file
        for line in file:
            # Convert the JSON string to a Python dictionary
            data = json.loads(line)
            # sqll = data['sql'].split(';')[0]
            # table_n = data['topic'].replace(' ', '').replace('-', '')
            # qtype = data['type']

            prompt = '''
                The question is {}, answers the question, just output the the answer, no other words.
                '''.format(data['question'])
            answe = agent(model, prompt)
            checkout = check(data['question'], data['goldanswer'], answe)
            if checkout == 'True':
                cout_t += 1
            ques_i = {'qid': idx, 'topic': data['topic'], 'edge': data['edge'], 'class': data['class'],
                      'hop': data['hop'], 'properties': data['properties'], 'type': data['type'],
                      'question': data['question'], 'sql': data['sql'], 'answer': answe, 'goldanswer': data['goldanswer'],'true': checkout}
            idx += 1
            fout_qa.write(json.dumps(ques_i) + '\n')

    fout_qa.close()
    acc = cout_t / idx
    print('The acc of GPT: {}'.format(acc))

def llama_eval(path, outfile='multi_entities_llama.jsonl', qfile='multi_entities_query.jsonl', model='llama3'):
    idx = 0
    cout_t = 0
    fout_qa = open(os.path.join(path, outfile), 'a')
    with open(os.path.join(path, qfile), 'r') as file:
        # Read each line in the file
        for line in file:
            # Convert the JSON string to a Python dictionary
            data = json.loads(line)

            prompt = '''
                The question is {}, answers the question, just output the the answer, no other words.
                '''.format(data['question'])
            answe = agent(model, prompt)

            checkout = check(data['question'], data['goldanswer'], answe)
            if checkout == 'True':
                cout_t += 1
            ques_i = {'qid': idx, 'topic': data['topic'], 'edge': data['edge'], 'class': data['class'],
                      'hop': data['hop'], 'properties': data['properties'], 'type': data['type'],
                      'question': data['question'], 'sql': data['sql'], 'answer': answe, 'goldanswer': data['goldanswer'],'true': checkout}
            idx += 1
            fout_qa.write(json.dumps(ques_i) + '\n')

    fout_qa.close()
    acc = cout_t/idx
    print('The acc of llama3: {}'.format(acc))

def sgraph(path, outfile='multi_entities_SQA.jsonl', qfile='multi_entities_query.jsonl'):
    idx = 0
    cout_t = 0
    fout_qa = open(os.path.join(path, outfile), 'a')
    with open(os.path.join(path, qfile), 'r') as file:
        # Read each line in the file
        for line in file:
            # Convert the JSON string to a Python dictionary
            data = json.loads(line)
            sqll = data['sql'].split(';')[0]
            table_n = data['topic'].replace(' ', '').replace('-', '')
            qtype = data['type']

            if ('Correlation Analysis' in qtype) or ('Variance Analysis' in qtype):
                prompt = '''
                The question is {}, Let's say we have a table that answers the question, just output the columns for the answer and related analysis method, no other words.
                '''.format(data['question'])
                answe = agent('GPT-4', prompt)
            else:
                # print(ques)
                # sql_query = "SELECT * FROM {};".format(table_n)
                answe = dbutils.csv_sqltool(table_n, sqll)
                # print(sqltool(sql_query))
                # print(answe)
                # if result_df != '[]':
            checkout = check(data['question'], data['goldanswer'], answe)
            if checkout == 'True':
                cout_t += 1
            ques_i = {'qid': idx, 'topic': data['topic'], 'edge': data['edge'], 'class': data['class'],
                      'hop': data['hop'], 'properties': data['properties'], 'type': data['type'],
                      'question': data['question'], 'sql': data['sql'], 'answer': answe, 'goldanswer': data['goldanswer'],'true': checkout}
            idx += 1
            fout_qa.write(json.dumps(ques_i) + '\n')

    fout_qa.close()
    acc = cout_t / idx
    print('The acc of SQA: {}'.format(acc))

def main():
    path = 'benchmark'

    gpt_eval(path)

    llama_eval(path)

    sgraph(path)


if __name__ == '__main__':
    main()